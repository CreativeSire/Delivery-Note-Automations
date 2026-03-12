from __future__ import annotations

import os
import secrets
import threading
from io import BytesIO
from pathlib import Path

from flask import Flask, abort, flash, jsonify, redirect, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

from loading_tracker_services import (
    LoadingTrackerError,
    PENDING_SENTINEL,
    build_loading_tracker_count_context,
    build_loading_tracker_overview,
    build_loading_tracker_day_context,
    build_loading_tracker_fees_context,
    build_loading_tracker_history_context,
    build_loading_tracker_inventory_context,
    build_loading_tracker_pending_context,
    build_loading_tracker_row_editor,
    build_loading_tracker_summary,
    build_loading_tracker_template_context,
    build_loading_tracker_template_summary,
    carry_forward_loading_tracker_week,
    capture_loading_tracker_template,
    create_loading_tracker_import_job,
    create_delivery_note_run_from_loading_day,
    create_loading_tracker_week_from_template,
    export_loading_tracker_history_csv,
    get_active_loading_tracker_import_job,
    get_loading_tracker_day,
    get_loading_tracker_import,
    get_loading_tracker_import_job,
    get_loading_tracker_row,
    get_loading_tracker_template,
    get_pending_reason_options,
    import_loading_tracker_workbook,
    move_loading_tracker_row,
    reset_loading_tracker_workspace,
    run_loading_tracker_import_job,
    save_loading_tracker_day_counts,
    save_inventory_adjustment,
    save_loading_tracker_row,
    serialize_loading_tracker_import_job,
)
from models import Product, db
from services import (
    ServiceError,
    WorkbookShapeError,
    apply_review_decisions,
    bootstrap_seed_uom_if_empty,
    build_dashboard_summary,
    build_ignored_history_summary,
    build_run_summary,
    create_tracker_run,
    export_ignored_history_to_xls,
    export_run_to_xls,
    import_uom_workbook,
    mark_source_sku_inactive,
    save_product_master_entry,
    set_product_active,
)
from workflow_services import (
    WorkflowError,
    apply_sales_order_review_decisions,
    apply_sku_automator_review_decisions,
    build_sales_order_run_summary,
    build_sales_order_summary,
    build_sku_automator_run_summary,
    build_sku_automator_summary,
    create_sales_order_run,
    create_sku_automator_run,
    export_sales_order_run_to_workbook,
    export_sku_automator_run_to_workbook,
)

APP_TIMEZONE = os.environ.get("APP_TIMEZONE", "Africa/Lagos")


def create_app(test_config: dict | None = None) -> Flask:
    app = Flask(__name__, instance_relative_config=True)
    Path(app.instance_path).mkdir(parents=True, exist_ok=True)

    app.config.update(
        SECRET_KEY=os.environ.get("SECRET_KEY", secrets.token_hex(24)),
        APP_TIMEZONE=APP_TIMEZONE,
        ALERT_EMAILS=os.environ.get("ALERT_EMAILS", ""),
        MAIL_HOST=os.environ.get("MAIL_HOST", ""),
        MAIL_PORT=int(os.environ.get("MAIL_PORT", "587")),
        MAIL_USERNAME=os.environ.get("MAIL_USERNAME", ""),
        MAIL_PASSWORD=os.environ.get("MAIL_PASSWORD", ""),
        MAIL_FROM=os.environ.get("MAIL_FROM", ""),
        MAIL_USE_TLS=os.environ.get("MAIL_USE_TLS", "true").lower() != "false",
        LOADING_TRACKER_IMPORT_SYNC=os.environ.get("LOADING_TRACKER_IMPORT_SYNC", "").lower() == "true",
        SQLALCHEMY_DATABASE_URI=_database_uri(app.instance_path),
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )
    if test_config:
        app.config.update(test_config)

    db.init_app(app)
    with app.app_context():
        db.create_all()
        if not app.config.get("TESTING"):
            bootstrap_seed_uom_if_empty()

    @app.context_processor
    def inject_app_summary() -> dict[str, object]:
        return {
            "app_summary": build_dashboard_summary(),
            "loading_summary": build_loading_tracker_summary(),
            "loading_template_summary": build_loading_tracker_template_summary(),
        }

    @app.get("/")
    def index() -> str:
        summary = build_dashboard_summary()
        loading_summary = build_loading_tracker_summary()
        latest_tracker_import = loading_summary.latest_import
        tracker_overview = build_loading_tracker_overview(latest_tracker_import)
        return render_template(
            "operations_dashboard.html",
            summary=summary,
            loading_summary=loading_summary,
            tracker_overview=tracker_overview,
            latest_tracker_import=latest_tracker_import,
        )

    @app.get("/sales-order")
    def sales_order_home() -> str:
        summary = build_sales_order_summary()
        return render_template("sales_order_home.html", summary=summary)

    @app.post("/sales-order/import")
    def upload_sales_order() -> str:
        uploaded_file = request.files.get("sales_order_workbook")
        if uploaded_file is None or uploaded_file.filename == "":
            flash("Please upload the Pep-up order workbook first.", "error")
            return redirect(url_for("sales_order_home"))

        try:
            run = create_sales_order_run(uploaded_file)
        except WorkflowError as exc:
            flash(str(exc), "error")
            return redirect(url_for("sales_order_home"))

        if run.row_count == 0:
            flash("No usable order rows were found in the uploaded workbook.", "error")
            return redirect(url_for("sales_order_home"))
        if run.rows_needing_review > 0:
            flash("Sales Order uploaded. Some SKUs need review before export.", "warning")
            return redirect(url_for("review_sales_order_run", run_id=run.id))

        flash("Sales Order run created and ready for Tally export.", "success")
        return redirect(url_for("view_sales_order_run", run_id=run.id))

    @app.get("/sales-order/runs/<run_id>")
    def view_sales_order_run(run_id: str) -> str:
        summary = build_sales_order_run_summary(run_id)
        if summary is None:
            abort(404)
        return render_template("sales_order_run_detail.html", summary=summary)

    @app.get("/sales-order/runs/<run_id>/review")
    def review_sales_order_run(run_id: str) -> str:
        summary = build_sales_order_run_summary(run_id)
        if summary is None:
            abort(404)
        all_products = list(
            db.session.query(Product).filter(Product.is_active.is_(True)).order_by(Product.sku_name.asc())
        )
        return render_template("sales_order_review.html", summary=summary, all_products=all_products)

    @app.post("/sales-order/runs/<run_id>/review")
    def submit_sales_order_review(run_id: str) -> str:
        summary = build_sales_order_run_summary(run_id)
        if summary is None:
            abort(404)

        mapping = {}
        for group in summary.unresolved_groups:
            raw_value = request.form.get(f"resolution::{group.source_sku}", "").strip()
            if not raw_value:
                flash(f"Please choose the correct product for '{group.source_sku}'.", "error")
                all_products = list(
                    db.session.query(Product).filter(Product.is_active.is_(True)).order_by(Product.sku_name.asc())
                )
                return render_template("sales_order_review.html", summary=summary, all_products=all_products)
            mapping[group.source_sku] = int(raw_value)

        try:
            run = apply_sales_order_review_decisions(run_id, mapping)
        except WorkflowError as exc:
            flash(str(exc), "error")
            return redirect(url_for("review_sales_order_run", run_id=run_id))

        flash(f"Review saved. {run.rows_ready} rows are now ready for export.", "success")
        return redirect(url_for("view_sales_order_run", run_id=run_id))

    @app.get("/sales-order/runs/<run_id>/download")
    def download_sales_order_run(run_id: str):
        try:
            filename, payload = export_sales_order_run_to_workbook(run_id)
        except WorkflowError as exc:
            flash(str(exc), "error")
            return redirect(url_for("view_sales_order_run", run_id=run_id))

        return send_file(
            BytesIO(payload),
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    @app.get("/sku-automator")
    def sku_automator_home() -> str:
        summary = build_sku_automator_summary()
        return render_template("sku_automator_home.html", summary=summary)

    @app.post("/sku-automator/import")
    def upload_sku_automator() -> str:
        uploaded_file = request.files.get("sku_automator_workbook")
        if uploaded_file is None or uploaded_file.filename == "":
            flash("Please upload the Tally sales-order export first.", "error")
            return redirect(url_for("sku_automator_home"))

        try:
            run = create_sku_automator_run(uploaded_file)
        except WorkflowError as exc:
            flash(str(exc), "error")
            return redirect(url_for("sku_automator_home"))

        if run.line_count == 0:
            flash("No voucher lines were found in the uploaded Tally export.", "error")
            return redirect(url_for("sku_automator_home"))
        if run.rows_needing_review > 0:
            flash("SKU Automator run created. Some SKUs need review before export.", "warning")
            return redirect(url_for("review_sku_automator_run", run_id=run.id))

        flash("SKU Automator run created and ready for planner output.", "success")
        return redirect(url_for("view_sku_automator_run", run_id=run.id))

    @app.get("/sku-automator/runs/<run_id>")
    def view_sku_automator_run(run_id: str) -> str:
        summary = build_sku_automator_run_summary(run_id)
        if summary is None:
            abort(404)
        return render_template("sku_automator_run_detail.html", summary=summary)

    @app.get("/sku-automator/runs/<run_id>/review")
    def review_sku_automator_run(run_id: str) -> str:
        summary = build_sku_automator_run_summary(run_id)
        if summary is None:
            abort(404)
        all_products = list(
            db.session.query(Product).filter(Product.is_active.is_(True)).order_by(Product.sku_name.asc())
        )
        return render_template("sku_automator_review.html", summary=summary, all_products=all_products)

    @app.post("/sku-automator/runs/<run_id>/review")
    def submit_sku_automator_review(run_id: str) -> str:
        summary = build_sku_automator_run_summary(run_id)
        if summary is None:
            abort(404)

        mapping = {}
        for group in summary.unresolved_groups:
            raw_value = request.form.get(f"resolution::{group.source_sku}", "").strip()
            if not raw_value:
                flash(f"Please choose the correct product for '{group.source_sku}'.", "error")
                all_products = list(
                    db.session.query(Product).filter(Product.is_active.is_(True)).order_by(Product.sku_name.asc())
                )
                return render_template("sku_automator_review.html", summary=summary, all_products=all_products)
            mapping[group.source_sku] = int(raw_value)

        try:
            run = apply_sku_automator_review_decisions(run_id, mapping)
        except WorkflowError as exc:
            flash(str(exc), "error")
            return redirect(url_for("review_sku_automator_run", run_id=run_id))

        flash(f"Review saved. {run.rows_ready} rows are now ready for export.", "success")
        return redirect(url_for("view_sku_automator_run", run_id=run_id))

    @app.get("/sku-automator/runs/<run_id>/download")
    def download_sku_automator_run(run_id: str):
        try:
            filename, payload = export_sku_automator_run_to_workbook(run_id)
        except WorkflowError as exc:
            flash(str(exc), "error")
            return redirect(url_for("view_sku_automator_run", run_id=run_id))

        return send_file(
            BytesIO(payload),
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    @app.get("/delivery-note")
    def delivery_note_home() -> str:
        summary = build_dashboard_summary()
        total_products = summary.product_count + summary.inactive_product_count
        chart = {
            "active_ratio": round((summary.product_count / total_products) * 100) if total_products else 0,
            "import_max": max((item.product_count for item in summary.recent_imports), default=1),
            "run_max": max((run.rows_detected for run in summary.recent_runs), default=1),
        }
        return render_template("dashboard.html", summary=summary, chart=chart)

    @app.get("/database")
    @app.get("/products")
    def product_master() -> str:
        active_products = list(
            db.session.query(Product).filter(Product.is_active.is_(True)).order_by(Product.sku_name.asc())
        )
        inactive_products = list(
            db.session.query(Product).filter(Product.is_active.is_(False)).order_by(Product.sku_name.asc()).limit(20)
        )
        summary = build_dashboard_summary()
        return render_template(
            "product_master.html",
            active_products=active_products,
            inactive_products=inactive_products,
            summary=summary,
            product_to_edit=None,
        )

    @app.post("/products")
    def create_product() -> str:
        try:
            product = save_product_master_entry(dict(request.form))
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for("product_master"))

        flash(f"'{product.sku_name}' was added to the product master.", "success")
        return redirect(url_for("product_master"))

    @app.get("/products/<int:product_id>/edit")
    def edit_product(product_id: int) -> str:
        product = db.session.get(Product, product_id)
        if product is None:
            abort(404)

        active_products = list(
            db.session.query(Product).filter(Product.is_active.is_(True)).order_by(Product.sku_name.asc())
        )
        inactive_products = list(
            db.session.query(Product).filter(Product.is_active.is_(False)).order_by(Product.sku_name.asc()).limit(20)
        )
        summary = build_dashboard_summary()
        return render_template(
            "product_master.html",
            active_products=active_products,
            inactive_products=inactive_products,
            summary=summary,
            product_to_edit=product,
        )

    @app.post("/products/<int:product_id>/edit")
    def update_product(product_id: int) -> str:
        try:
            product = save_product_master_entry(dict(request.form), product_id=product_id)
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for("edit_product", product_id=product_id))

        flash(f"'{product.sku_name}' was updated.", "success")
        return redirect(url_for("product_master"))

    @app.post("/products/<int:product_id>/deactivate")
    def deactivate_product(product_id: int) -> str:
        try:
            product = set_product_active(product_id, False)
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for("product_master"))

        flash(f"'{product.sku_name}' was removed from the active product master.", "warning")
        return redirect(url_for("product_master"))

    @app.post("/products/<int:product_id>/activate")
    def activate_product(product_id: int) -> str:
        try:
            product = set_product_active(product_id, True)
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for("product_master"))

        flash(f"'{product.sku_name}' is active again.", "success")
        return redirect(url_for("product_master"))

    @app.post("/uom/import")
    def upload_uom() -> str:
        uploaded_file = request.files.get("uom_workbook")
        if uploaded_file is None or uploaded_file.filename == "":
            flash("Please choose the updated UOM workbook first.", "error")
            return redirect(url_for("delivery_note_home"))

        try:
            import_log = import_uom_workbook(uploaded_file)
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for("delivery_note_home"))

        skipped = getattr(import_log, "skipped_count", 0)
        deactivated = getattr(import_log, "deactivated_count", 0)
        if skipped or deactivated:
            parts = [f"{import_log.product_count} new product rows were added"]
            if skipped:
                parts.append(f"{skipped} existing items were skipped")
            if deactivated:
                parts.append(f"{deactivated} items were moved to inactive")
            flash(
                f"UOM update complete. {' and '.join(parts)}.",
                "success",
            )
        else:
            flash(f"UOM import complete. {import_log.product_count} product rows were saved.", "success")
        return redirect(url_for("delivery_note_home"))

    @app.post("/runs/import")
    def upload_tracker() -> str:
        uploaded_file = request.files.get("tracker_workbook")
        if uploaded_file is None or uploaded_file.filename == "":
            flash("Please upload the loading tracker workbook first.", "error")
            return redirect(url_for("delivery_note_home"))

        try:
            run = create_tracker_run(uploaded_file, app.config["APP_TIMEZONE"])
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for("delivery_note_home"))

        if run.rows_detected == 0:
            flash("No rows above 0.00 were found in the tracker file.", "error")
            return redirect(url_for("delivery_note_home"))

        if run.rows_needing_review > 0:
            flash("Tracker uploaded. Some SKUs need review before export.", "warning")
            return redirect(url_for("review_run", run_id=run.id))

        flash("Tracker uploaded and matched successfully.", "success")
        return redirect(url_for("view_run", run_id=run.id))

    @app.get("/runs/<run_id>")
    def view_run(run_id: str) -> str:
        summary = build_run_summary(run_id)
        if summary is None:
            abort(404)
        ignored_history = build_ignored_history_summary(run_id)
        return render_template("run_detail.html", summary=summary, ignored_history=ignored_history)

    @app.get("/runs/<run_id>/review")
    def review_run(run_id: str) -> str:
        summary = build_run_summary(run_id)
        if summary is None:
            abort(404)
        all_products = list(
            db.session.query(Product).filter(Product.is_active.is_(True)).order_by(Product.sku_name.asc())
        )
        return render_template("review.html", summary=summary, all_products=all_products)

    @app.post("/runs/<run_id>/review")
    def submit_review(run_id: str) -> str:
        summary = build_run_summary(run_id)
        if summary is None:
            abort(404)

        mark_inactive_sku = request.form.get("mark_inactive", "").strip()
        if mark_inactive_sku:
            try:
                run, product = mark_source_sku_inactive(run_id, mark_inactive_sku)
            except WorkbookShapeError as exc:
                flash(str(exc), "error")
                return redirect(url_for("review_run", run_id=run_id))

            flash(
                f"'{product.sku_name}' was moved to inactive. Future tracker runs will ignore it until a new source file brings it back.",
                "warning",
            )
            if run.rows_needing_review > 0:
                return redirect(url_for("review_run", run_id=run_id))
            return redirect(url_for("view_run", run_id=run_id))

        mapping = {}
        for group in summary.unresolved_groups:
            raw_value = request.form.get(f"resolution::{group.source_sku}", "").strip()
            if not raw_value:
                flash(f"Please choose the correct product for '{group.source_sku}'.", "error")
                all_products = list(
                    db.session.query(Product).filter(Product.is_active.is_(True)).order_by(Product.sku_name.asc())
                )
                return render_template("review.html", summary=summary, all_products=all_products)
            mapping[group.source_sku] = int(raw_value)

        try:
            run = apply_review_decisions(run_id, mapping)
        except WorkbookShapeError as exc:
            flash(str(exc), "error")
            return redirect(url_for("review_run", run_id=run_id))

        flash(f"Review saved. {run.rows_ready} rows are now ready for export.", "success")
        return redirect(url_for("view_run", run_id=run_id))

    @app.get("/runs/<run_id>/download")
    def download_run(run_id: str):
        try:
            filename, payload = export_run_to_xls(run_id)
        except WorkbookShapeError as exc:
            flash(str(exc), "error")
            return redirect(url_for("view_run", run_id=run_id))

        return send_file(
            BytesIO(payload),
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.ms-excel",
        )

    @app.get("/runs/<run_id>/ignored/download")
    def download_ignored_history(run_id: str):
        try:
            filename, payload = export_ignored_history_to_xls(run_id)
        except WorkbookShapeError as exc:
            flash(str(exc), "error")
            return redirect(url_for("view_run", run_id=run_id))

        return send_file(
            BytesIO(payload),
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.ms-excel",
        )

    @app.get("/loading-tracker")
    def loading_tracker_home() -> str:
        tracker_import = get_loading_tracker_import()
        overview = build_loading_tracker_overview(tracker_import)
        day_cards = tracker_import.days if tracker_import is not None else []
        template = get_loading_tracker_template()
        active_import_job = get_loading_tracker_import_job(request.args.get("job")) if request.args.get("job") else None
        if active_import_job is None:
            active_import_job = get_active_loading_tracker_import_job()
        return render_template(
            "loading_tracker_home.html",
            tracker_import=tracker_import,
            overview=overview,
            day_cards=day_cards,
            template=template,
            template_context=build_loading_tracker_template_context(template),
            active_import_job=serialize_loading_tracker_import_job(active_import_job),
        )

    @app.post("/loading-tracker/reset")
    def loading_tracker_reset() -> str:
        try:
            cleared = reset_loading_tracker_workspace(app.instance_path)
        except LoadingTrackerError as exc:
            flash(str(exc), "error")
            return redirect(url_for("loading_tracker_home"))

        flash(
            "Loading Tracker was cleared for a clean re-import. "
            f"Removed {cleared['imports']} week import(s), {cleared['rows']} planning row(s), and {cleared['events']} event log(s). "
            "Your product master, UOM source, aliases, and Delivery Note records stayed intact.",
            "warning",
        )
        return redirect(url_for("loading_tracker_home"))

    @app.post("/loading-tracker/imports/<import_id>/carry-forward")
    def loading_tracker_carry_forward(import_id: str) -> str:
        try:
            tracker_import = carry_forward_loading_tracker_week(import_id)
        except LoadingTrackerError as exc:
            flash(str(exc), "error")
            return redirect(url_for("loading_tracker_import_view", import_id=import_id))

        flash("A fresh week was created and the remaining G2G plus pending rows were carried forward.", "success")
        return redirect(url_for("loading_tracker_import_view", import_id=tracker_import.id))

    @app.get("/loading-tracker/imports/<import_id>")
    def loading_tracker_import_view(import_id: str) -> str:
        tracker_import = get_loading_tracker_import(import_id)
        if tracker_import is None:
            abort(404)
        overview = build_loading_tracker_overview(tracker_import)
        template = get_loading_tracker_template()
        return render_template(
            "loading_tracker_home.html",
            tracker_import=tracker_import,
            overview=overview,
            day_cards=tracker_import.days,
            template=template,
            template_context=build_loading_tracker_template_context(template),
            active_import_job=serialize_loading_tracker_import_job(get_active_loading_tracker_import_job()),
        )

    @app.post("/loading-tracker/template/capture")
    def loading_tracker_capture_template() -> str:
        source_import_id = request.form.get("source_import_id", "").strip() or None
        template_name = request.form.get("template_name", "").strip() or None
        try:
            template = capture_loading_tracker_template(source_import_id, name=template_name)
        except LoadingTrackerError as exc:
            flash(str(exc), "error")
            return redirect(url_for("loading_tracker_home"))

        flash(
            f"'{template.name}' is now the active backend planning template. Future weeks will use it by default.",
            "success",
        )
        target_import_id = source_import_id or (get_loading_tracker_import().id if get_loading_tracker_import() else None)
        if target_import_id:
            return redirect(url_for("loading_tracker_import_view", import_id=target_import_id))
        return redirect(url_for("loading_tracker_home"))

    @app.post("/loading-tracker/template/start-week")
    def loading_tracker_start_week_from_template() -> str:
        template_id = request.form.get("template_id", "").strip() or None
        source_import_id = request.form.get("source_import_id", "").strip() or None
        try:
            tracker_import = create_loading_tracker_week_from_template(template_id, source_import_id=source_import_id)
        except LoadingTrackerError as exc:
            flash(str(exc), "error")
            return redirect(url_for("loading_tracker_home"))

        flash(
            "A new live week was created from the backend template. Remaining stock and pending lines were carried forward where available.",
            "success",
        )
        return redirect(url_for("loading_tracker_import_view", import_id=tracker_import.id))

    @app.post("/loading-tracker/import")
    def upload_loading_tracker() -> str:
        uploaded_file = request.files.get("loading_tracker_workbook")
        if uploaded_file is None or uploaded_file.filename == "":
            flash("Please choose the weekly loading tracker workbook first.", "error")
            return redirect(url_for("loading_tracker_home"))

        filename = uploaded_file.filename or "loading-tracker.xlsx"
        job = create_loading_tracker_import_job(filename)
        job_id = job.id
        saved_path = _save_loading_tracker_upload(app.instance_path, job_id, uploaded_file)

        if app.config.get("TESTING") or app.config.get("LOADING_TRACKER_IMPORT_SYNC"):
            run_loading_tracker_import_job(job_id, saved_path, filename)
        else:
            thread = threading.Thread(
                target=_run_loading_tracker_import_job_in_background,
                args=(app, job_id, saved_path, filename),
                daemon=True,
            )
            thread.start()

        if _wants_json():
            return (
                jsonify(
                    {
                        "job": serialize_loading_tracker_import_job(get_loading_tracker_import_job(job_id)),
                        "status_url": url_for("loading_tracker_import_job_status", job_id=job_id),
                    }
                ),
                202,
            )

        flash("Weekly tracker import started in the background. We will keep building the live week for you.", "success")
        return redirect(url_for("loading_tracker_home", job=job_id))

    @app.get("/loading-tracker/jobs/<job_id>")
    def loading_tracker_import_job_status(job_id: str):
        job = get_loading_tracker_import_job(job_id)
        if job is None:
            abort(404)

        payload = serialize_loading_tracker_import_job(job) or {}
        if job.tracker_import_id:
            payload["redirect_url"] = url_for("loading_tracker_import_view", import_id=job.tracker_import_id)
        return jsonify(payload)

    @app.get("/loading-tracker/imports/<import_id>/days/<day_name>")
    def loading_tracker_day_view(import_id: str, day_name: str) -> str:
        tracker_import = get_loading_tracker_import(import_id)
        if tracker_import is None:
            abort(404)
        day = get_loading_tracker_day(import_id, day_name)
        if day is None:
            abort(404)
        return render_template(
            "loading_tracker_day.html",
            tracker_import=tracker_import,
            day=day,
            day_context=build_loading_tracker_day_context(day),
            count_context=build_loading_tracker_count_context(day),
            pending_reason_options=get_pending_reason_options(),
        )

    @app.get("/loading-tracker/imports/<import_id>/days/<day_name>/counts")
    def loading_tracker_day_counts_view(import_id: str, day_name: str) -> str:
        tracker_import = get_loading_tracker_import(import_id)
        if tracker_import is None:
            abort(404)
        day = get_loading_tracker_day(import_id, day_name)
        if day is None:
            abort(404)
        return render_template(
            "loading_tracker_counts.html",
            tracker_import=tracker_import,
            day=day,
            count_context=build_loading_tracker_count_context(day),
        )

    @app.post("/loading-tracker/imports/<import_id>/days/<day_name>/counts")
    def loading_tracker_day_counts_save(import_id: str, day_name: str) -> str:
        try:
            save_loading_tracker_day_counts(import_id, day_name, dict(request.form))
        except LoadingTrackerError as exc:
            flash(str(exc), "error")
            return redirect(url_for("loading_tracker_day_counts_view", import_id=import_id, day_name=day_name))

        flash(f"Start-of-day physical count saved for {day_name}. Any discrepancies are now visible in the planner.", "success")
        return redirect(url_for("loading_tracker_day_view", import_id=import_id, day_name=day_name))

    @app.post("/loading-tracker/imports/<import_id>/days/<day_name>/handoff")
    def loading_tracker_day_handoff(import_id: str, day_name: str) -> str:
        try:
            run = create_delivery_note_run_from_loading_day(import_id, day_name, app.config["APP_TIMEZONE"])
        except LoadingTrackerError as exc:
            flash(str(exc), "error")
            return redirect(url_for("loading_tracker_day_view", import_id=import_id, day_name=day_name))

        if run.rows_needing_review > 0:
            flash("The day plan was sent to Delivery Note, but some SKUs still need review.", "warning")
            return redirect(url_for("review_run", run_id=run.id))

        flash("The final adjusted day plan has been handed off to Delivery Note.", "success")
        return redirect(url_for("view_run", run_id=run.id))

    @app.get("/loading-tracker/imports/<import_id>/days/<day_name>/new")
    def loading_tracker_day_new_row(import_id: str, day_name: str) -> str:
        tracker_import = get_loading_tracker_import(import_id)
        if tracker_import is None:
            abort(404)
        editor = build_loading_tracker_row_editor(tracker_import, selected_day_name=day_name)
        return render_template(
            "loading_tracker_row_form.html",
            tracker_import=tracker_import,
            editor=editor,
            page_title=f"Add {day_name} planning row",
            back_target=url_for("loading_tracker_day_view", import_id=import_id, day_name=day_name),
        )

    @app.post("/loading-tracker/imports/<import_id>/days/<day_name>/new")
    def loading_tracker_day_create_row(import_id: str, day_name: str) -> str:
        try:
            row = save_loading_tracker_row(import_id, dict(request.form))
        except LoadingTrackerError as exc:
            flash(str(exc), "error")
            return redirect(url_for("loading_tracker_day_new_row", import_id=import_id, day_name=day_name))

        flash(f"'{row.store_name}' was added to the live planner.", "success")
        if row.day is not None:
            return redirect(url_for("loading_tracker_day_view", import_id=import_id, day_name=row.day.day_name))
        return redirect(url_for("loading_tracker_pending_view", import_id=import_id))

    @app.get("/loading-tracker/imports/<import_id>/rows/<int:row_id>/edit")
    def loading_tracker_row_edit(import_id: str, row_id: int) -> str:
        tracker_import = get_loading_tracker_import(import_id)
        if tracker_import is None:
            abort(404)
        row = get_loading_tracker_row(row_id)
        if row is None or row.tracker_import_id != import_id:
            abort(404)
        editor = build_loading_tracker_row_editor(tracker_import, row=row)
        back_target = (
            url_for("loading_tracker_day_view", import_id=import_id, day_name=row.day.day_name)
            if row.day is not None
            else url_for("loading_tracker_pending_view", import_id=import_id)
        )
        return render_template(
            "loading_tracker_row_form.html",
            tracker_import=tracker_import,
            editor=editor,
            page_title=f"Edit {row.store_name}",
            back_target=back_target,
        )

    @app.post("/loading-tracker/imports/<import_id>/rows/<int:row_id>/edit")
    def loading_tracker_row_update(import_id: str, row_id: int) -> str:
        try:
            row = save_loading_tracker_row(import_id, dict(request.form), row_id=row_id)
        except LoadingTrackerError as exc:
            flash(str(exc), "error")
            return redirect(url_for("loading_tracker_row_edit", import_id=import_id, row_id=row_id))

        flash(f"'{row.store_name}' was updated.", "success")
        if row.day is not None:
            return redirect(url_for("loading_tracker_day_view", import_id=import_id, day_name=row.day.day_name))
        return redirect(url_for("loading_tracker_pending_view", import_id=import_id))

    @app.post("/loading-tracker/imports/<import_id>/rows/<int:row_id>/move")
    def loading_tracker_row_move(import_id: str, row_id: int) -> str:
        target_day_name = request.form.get("target_day_name", "").strip() or PENDING_SENTINEL
        reason_code = request.form.get("reason_code", "").strip() or None
        reason_note = request.form.get("reason_note", "").strip() or None
        try:
            row = move_loading_tracker_row(import_id, row_id, target_day_name, reason_code, reason_note)
        except LoadingTrackerError as exc:
            flash(str(exc), "error")
            return redirect(request.referrer or url_for("loading_tracker_import_view", import_id=import_id))

        if row.day is not None:
            flash(f"'{row.store_name}' moved into {row.day.day_name}.", "success")
            return redirect(url_for("loading_tracker_day_view", import_id=import_id, day_name=row.day.day_name))

        flash(f"'{row.store_name}' is now waiting in Pending.", "warning")
        return redirect(url_for("loading_tracker_pending_view", import_id=import_id))

    @app.get("/loading-tracker/imports/<import_id>/pending")
    def loading_tracker_pending_view(import_id: str) -> str:
        tracker_import = get_loading_tracker_import(import_id)
        if tracker_import is None:
            abort(404)
        return render_template(
            "loading_tracker_pending.html",
            tracker_import=tracker_import,
            pending_context=build_loading_tracker_pending_context(tracker_import),
        )

    @app.get("/loading-tracker/imports/<import_id>/inventory")
    def loading_tracker_inventory_view(import_id: str) -> str:
        tracker_import = get_loading_tracker_import(import_id)
        if tracker_import is None:
            abort(404)
        return render_template(
            "loading_tracker_inventory.html",
            tracker_import=tracker_import,
            inventory_context=build_loading_tracker_inventory_context(tracker_import),
        )

    @app.post("/loading-tracker/imports/<import_id>/inventory")
    def loading_tracker_inventory_update(import_id: str) -> str:
        try:
            item = save_inventory_adjustment(import_id, dict(request.form))
        except LoadingTrackerError as exc:
            flash(str(exc), "error")
            return redirect(url_for("loading_tracker_inventory_view", import_id=import_id))

        flash(f"Inventory for '{item.sku_name}' was updated.", "success")
        return redirect(url_for("loading_tracker_inventory_view", import_id=import_id))

    @app.get("/loading-tracker/imports/<import_id>/fees")
    def loading_tracker_fees_view(import_id: str) -> str:
        tracker_import = get_loading_tracker_import(import_id)
        if tracker_import is None:
            abort(404)
        return render_template(
            "loading_tracker_fees.html",
            tracker_import=tracker_import,
            fees_context=build_loading_tracker_fees_context(tracker_import),
        )

    @app.get("/loading-tracker/imports/<import_id>/history")
    def loading_tracker_history_view(import_id: str) -> str:
        tracker_import = get_loading_tracker_import(import_id)
        if tracker_import is None:
            abort(404)
        return render_template(
            "loading_tracker_history.html",
            tracker_import=tracker_import,
            history_context=build_loading_tracker_history_context(tracker_import),
        )

    @app.get("/loading-tracker/imports/<import_id>/history/download")
    def loading_tracker_history_download(import_id: str):
        try:
            filename, payload = export_loading_tracker_history_csv(import_id)
        except LoadingTrackerError as exc:
            flash(str(exc), "error")
            return redirect(url_for("loading_tracker_history_view", import_id=import_id))

        return send_file(
            BytesIO(payload),
            as_attachment=True,
            download_name=filename,
            mimetype="text/csv",
        )

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


def _run_loading_tracker_import_job_in_background(
    app: Flask,
    job_id: str,
    saved_path: Path,
    filename: str,
) -> None:
    with app.app_context():
        run_loading_tracker_import_job(job_id, saved_path, filename)


def _save_loading_tracker_upload(instance_path: str, job_id: str, uploaded_file) -> Path:
    upload_root = Path(instance_path) / "loading_tracker_jobs"
    upload_root.mkdir(parents=True, exist_ok=True)
    safe_name = secure_filename(uploaded_file.filename or "loading-tracker.xlsx") or "loading-tracker.xlsx"
    saved_path = upload_root / f"{job_id}-{safe_name}"
    uploaded_file.save(saved_path)
    return saved_path


def _wants_json() -> bool:
    return request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.accept_mimetypes.best == "application/json"


def _database_uri(instance_path: str) -> str:
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if database_url:
        if database_url.startswith("postgres://"):
            return database_url.replace("postgres://", "postgresql+psycopg://", 1)
        if database_url.startswith("postgresql://") and not database_url.startswith("postgresql+psycopg://"):
            return database_url.replace("postgresql://", "postgresql+psycopg://", 1)
        return database_url
    return f"sqlite:///{Path(instance_path) / 'delivery_note.db'}"


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), debug=False)
