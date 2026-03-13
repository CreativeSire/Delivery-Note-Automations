from __future__ import annotations

import os
import secrets
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path

from flask import Flask, abort, flash, jsonify, redirect, render_template, request, send_file, url_for
from sqlalchemy import or_
from werkzeug.utils import secure_filename

from audit_services import build_audit_timeline, record_audit_event
from loading_tracker_services import (
    DAY_SHEET_NAMES,
    LoadingTrackerError,
    PENDING_SENTINEL,
    bulk_move_loading_tracker_rows,
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
    create_loading_tracker_week_from_sku_automator_run,
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
from models import BrandPartnerRule, Product, ProductAlias, UomImportReview, db, ensure_runtime_schema
from services import (
    ServiceError,
    WorkbookShapeError,
    apply_uom_import_review,
    apply_review_decisions,
    bootstrap_seed_uom_if_empty,
    build_dashboard_summary,
    build_invoice_routing_summary,
    build_ignored_history_summary,
    build_run_summary,
    create_tracker_run,
    discard_uom_import_review,
    export_ignored_history_to_xls,
    export_run_to_xls,
    get_pending_uom_import_review,
    get_uom_import_review,
    import_invoice_routing_workbook,
    import_uom_workbook,
    list_brand_partner_rules,
    mark_source_sku_inactive,
    preview_brand_partner_classification,
    save_product_master_entry,
    save_brand_partner_rule,
    set_brand_partner_rule_active,
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
        ensure_runtime_schema(db.engine)
        if not app.config.get("TESTING"):
            bootstrap_seed_uom_if_empty()

    @app.context_processor
    def inject_app_summary() -> dict[str, object]:
        return {
            "app_summary": build_dashboard_summary(),
            "loading_summary": build_loading_tracker_summary(),
            "loading_template_summary": build_loading_tracker_template_summary(),
        }

    def render_product_master(product_to_edit: Product | None = None) -> str:
        search_query = request.args.get("q", "").strip()
        summary = build_dashboard_summary()
        invoice_routing_summary = build_invoice_routing_summary()
        correction_matches: list[ProductAlias] = []
        brand_partner_rules = list_brand_partner_rules()
        pending_uom_review = get_pending_uom_import_review()

        active_query = db.session.query(Product).filter(Product.is_active.is_(True))
        inactive_query = db.session.query(Product).filter(Product.is_active.is_(False))

        if search_query:
            like_query = f"%{search_query}%"
            alias_product_ids = db.session.query(ProductAlias.product_id).filter(
                or_(
                    ProductAlias.alias_name.ilike(like_query),
                    ProductAlias.normalized_name.ilike(like_query),
                )
            )
            product_filter = or_(
                Product.sku_name.ilike(like_query),
                Product.normalized_name.ilike(like_query),
                Product.uom.ilike(like_query),
                Product.alt_uom.ilike(like_query),
                Product.id.in_(alias_product_ids),
            )
            active_products = list(active_query.filter(product_filter).order_by(Product.sku_name.asc()))
            inactive_products = list(inactive_query.filter(product_filter).order_by(Product.sku_name.asc()))
            correction_matches = list(
                db.session.query(ProductAlias)
                .join(Product)
                .filter(
                    or_(
                        ProductAlias.alias_name.ilike(like_query),
                        ProductAlias.normalized_name.ilike(like_query),
                        Product.sku_name.ilike(like_query),
                        Product.normalized_name.ilike(like_query),
                    )
                )
                .order_by(ProductAlias.alias_name.asc())
                .limit(60)
            )
        else:
            active_products = list(active_query.order_by(Product.sku_name.asc()))
            inactive_products = list(inactive_query.order_by(Product.sku_name.asc()).limit(20))

        return render_template(
            "product_master.html",
            active_products=active_products,
            inactive_products=inactive_products,
            correction_matches=correction_matches,
            brand_partner_rules=brand_partner_rules,
            pending_uom_review=pending_uom_review,
            inactive_preview_limited=not search_query and summary.inactive_product_count > len(inactive_products),
            search_query=search_query,
            summary=summary,
            invoice_routing_summary=invoice_routing_summary,
            product_to_edit=product_to_edit,
        )

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

    @app.get("/audit")
    def audit_timeline() -> str:
        selected_module = request.args.get("module", "").strip() or None
        timeline = build_audit_timeline(module_name=selected_module)
        return render_template("audit_timeline.html", timeline=timeline)

    @app.get("/bp-rules")
    def brand_partner_rules_home() -> str:
        search_query = request.args.get("q", "").strip()
        preview = None
        rules = list_brand_partner_rules()
        invoice_routing_summary = build_invoice_routing_summary()
        if search_query:
            lowered = search_query.lower()
            rules = [
                rule
                for rule in rules
                if lowered in (rule.sku_name_pattern or "").lower()
                or lowered in (rule.store_name_pattern or "").lower()
                or lowered in (rule.rule_name or "").lower()
            ]

        all_products = list(
            db.session.query(Product).filter(Product.is_active.is_(True)).order_by(Product.sku_name.asc())
        )
        return render_template(
            "bp_rules.html",
            rules=rules,
            search_query=search_query,
            preview=preview,
            all_products=all_products,
            invoice_routing_summary=invoice_routing_summary,
        )

    @app.post("/bp-rules/test")
    def preview_brand_partner_rule() -> str:
        search_query = request.form.get("q", "").strip()
        sku_name = request.form.get("sku_name", "").strip()
        store_name = request.form.get("store_name", "").strip()
        raw_reference_no = request.form.get("raw_reference_no", "").strip()
        product_id_raw = request.form.get("product_id", "").strip()
        product_id = int(product_id_raw) if product_id_raw.isdigit() else None

        rules = list_brand_partner_rules()
        if search_query:
            lowered = search_query.lower()
            rules = [
                rule
                for rule in rules
                if lowered in (rule.sku_name_pattern or "").lower()
                or lowered in (rule.store_name_pattern or "").lower()
                or lowered in (rule.rule_name or "").lower()
            ]
        all_products = list(
            db.session.query(Product).filter(Product.is_active.is_(True)).order_by(Product.sku_name.asc())
        )
        preview = preview_brand_partner_classification(
            sku_name=sku_name,
            store_name=store_name or None,
            raw_reference_no=raw_reference_no or None,
            product_id=product_id,
        )
        invoice_routing_summary = build_invoice_routing_summary()
        return render_template(
            "bp_rules.html",
            rules=rules,
            search_query=search_query,
            preview=preview,
            all_products=all_products,
            invoice_routing_summary=invoice_routing_summary,
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
            record_audit_event(
                module_name="Sales Order",
                event_type="run_created",
                entity_type="sales_order_run",
                entity_id=run.id,
                entity_name=run.original_filename,
                summary_text=f"Imported Sales Order workbook '{run.original_filename}' with {run.row_count} rows and {run.rows_needing_review} review item(s).",
                details={"row_count": run.row_count, "rows_ready": run.rows_ready, "rows_needing_review": run.rows_needing_review},
            )
            db.session.commit()
            flash("Sales Order uploaded. Some SKUs need review before export.", "warning")
            return redirect(url_for("review_sales_order_run", run_id=run.id))

        record_audit_event(
            module_name="Sales Order",
            event_type="run_created",
            entity_type="sales_order_run",
            entity_id=run.id,
            entity_name=run.original_filename,
            summary_text=f"Imported Sales Order workbook '{run.original_filename}' with {run.row_count} ready rows.",
            details={"row_count": run.row_count, "rows_ready": run.rows_ready, "rows_needing_review": run.rows_needing_review},
        )
        db.session.commit()
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

        record_audit_event(
            module_name="Sales Order",
            event_type="review_applied",
            entity_type="sales_order_run",
            entity_id=run.id,
            entity_name=run.original_filename,
            summary_text=f"Completed Sales Order review for '{run.original_filename}'. {run.rows_ready} row(s) are ready.",
            details={"rows_ready": run.rows_ready, "rows_needing_review": run.rows_needing_review},
        )
        db.session.commit()
        flash(f"Review saved. {run.rows_ready} rows are now ready for export.", "success")
        return redirect(url_for("view_sales_order_run", run_id=run_id))

    @app.get("/sales-order/runs/<run_id>/download")
    def download_sales_order_run(run_id: str):
        try:
            filename, payload = export_sales_order_run_to_workbook(run_id)
        except WorkflowError as exc:
            flash(str(exc), "error")
            return redirect(url_for("view_sales_order_run", run_id=run_id))

        record_audit_event(
            module_name="Sales Order",
            event_type="exported",
            entity_type="sales_order_run",
            entity_id=run_id,
            entity_name=filename,
            summary_text=f"Exported Sales Order workbook '{filename}'.",
        )
        db.session.commit()

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
            record_audit_event(
                module_name="SKU Automator",
                event_type="run_created",
                entity_type="sku_automator_run",
                entity_id=run.id,
                entity_name=run.original_filename,
                summary_text=f"Imported SKU Automator register '{run.original_filename}' with {run.line_count} lines and {run.rows_needing_review} review item(s).",
                details={"line_count": run.line_count, "rows_ready": run.rows_ready, "rows_needing_review": run.rows_needing_review},
            )
            db.session.commit()
            flash("SKU Automator run created. Some SKUs need review before export.", "warning")
            return redirect(url_for("review_sku_automator_run", run_id=run.id))

        record_audit_event(
            module_name="SKU Automator",
            event_type="run_created",
            entity_type="sku_automator_run",
            entity_id=run.id,
            entity_name=run.original_filename,
            summary_text=f"Imported SKU Automator register '{run.original_filename}' with {run.line_count} ready lines.",
            details={"line_count": run.line_count, "rows_ready": run.rows_ready, "rows_needing_review": run.rows_needing_review},
        )
        db.session.commit()
        flash("SKU Automator run created and ready for planner output.", "success")
        return redirect(url_for("view_sku_automator_run", run_id=run.id))

    @app.get("/sku-automator/runs/<run_id>")
    def view_sku_automator_run(run_id: str) -> str:
        summary = build_sku_automator_run_summary(run_id)
        if summary is None:
            abort(404)
        loading_tracker_template = get_loading_tracker_template()
        latest_loading_tracker_import = get_loading_tracker_import()
        if loading_tracker_template is not None and loading_tracker_template.days:
            loading_tracker_day_options = [day.day_name for day in loading_tracker_template.days]
        elif latest_loading_tracker_import is not None and latest_loading_tracker_import.days:
            loading_tracker_day_options = [day.day_name for day in latest_loading_tracker_import.days]
        else:
            loading_tracker_day_options = list(DAY_SHEET_NAMES)
        return render_template(
            "sku_automator_run_detail.html",
            summary=summary,
            loading_tracker_template=loading_tracker_template,
            latest_loading_tracker_import=latest_loading_tracker_import,
            loading_tracker_day_options=loading_tracker_day_options,
        )

    @app.post("/sku-automator/runs/<run_id>/loading-tracker")
    def create_loading_tracker_week_from_sku_run(run_id: str) -> str:
        target_day_name = request.form.get("target_day_name", "").strip() or None
        week_label = request.form.get("week_label", "").strip() or None
        source_import_id = request.form.get("source_import_id", "").strip() or None
        try:
            tracker_import = create_loading_tracker_week_from_sku_automator_run(
                run_id,
                target_day_name=target_day_name,
                source_import_id=source_import_id,
                week_label=week_label,
            )
        except LoadingTrackerError as exc:
            flash(str(exc), "error")
            return redirect(url_for("view_sku_automator_run", run_id=run_id))

        flash(
            "The SKU Automator matrix is now a live Loading Tracker week. Pending and remaining stock were carried forward where available.",
            "success",
        )
        record_audit_event(
            module_name="Loading Tracker",
            event_type="week_seeded_from_sku_automator",
            entity_type="loading_tracker_import",
            entity_id=tracker_import.id,
            entity_name=tracker_import.week_label,
            summary_text=f"Created Loading Tracker week '{tracker_import.week_label}' from SKU Automator run {run_id}.",
        )
        db.session.commit()
        return redirect(url_for("loading_tracker_import_view", import_id=tracker_import.id))

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

        record_audit_event(
            module_name="SKU Automator",
            event_type="review_applied",
            entity_type="sku_automator_run",
            entity_id=run.id,
            entity_name=run.original_filename,
            summary_text=f"Completed SKU Automator review for '{run.original_filename}'. {run.rows_ready} line(s) are ready.",
            details={"rows_ready": run.rows_ready, "rows_needing_review": run.rows_needing_review},
        )
        db.session.commit()
        flash(f"Review saved. {run.rows_ready} rows are now ready for export.", "success")
        return redirect(url_for("view_sku_automator_run", run_id=run_id))

    @app.get("/sku-automator/runs/<run_id>/download")
    def download_sku_automator_run(run_id: str):
        try:
            filename, payload = export_sku_automator_run_to_workbook(run_id)
        except WorkflowError as exc:
            flash(str(exc), "error")
            return redirect(url_for("view_sku_automator_run", run_id=run_id))

        record_audit_event(
            module_name="SKU Automator",
            event_type="exported",
            entity_type="sku_automator_run",
            entity_id=run_id,
            entity_name=filename,
            summary_text=f"Exported SKU Automator workbook '{filename}'.",
        )
        db.session.commit()

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
        return render_product_master()

    @app.post("/products")
    def create_product() -> str:
        search_query = request.form.get("q", "").strip()
        try:
            product = save_product_master_entry(dict(request.form))
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for("product_master", q=search_query) if search_query else url_for("product_master"))

        record_audit_event(
            module_name="Database",
            event_type="product_created",
            entity_type="product",
            entity_id=str(product.id),
            entity_name=product.sku_name,
            summary_text=f"Added '{product.sku_name}' to the product master.",
            details={"vatable": bool(product.vatable), "uom": product.uom or "", "price": str(product.price or "")},
        )
        db.session.commit()
        flash(f"'{product.sku_name}' was added to the product master.", "success")
        return redirect(url_for("product_master", q=search_query) if search_query else url_for("product_master"))

    @app.get("/products/<int:product_id>/edit")
    def edit_product(product_id: int) -> str:
        product = db.session.get(Product, product_id)
        if product is None:
            abort(404)

        return render_product_master(product_to_edit=product)

    @app.post("/products/<int:product_id>/edit")
    def update_product(product_id: int) -> str:
        search_query = request.form.get("q", "").strip()
        try:
            product = save_product_master_entry(dict(request.form), product_id=product_id)
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(
                url_for("edit_product", product_id=product_id, q=search_query)
                if search_query
                else url_for("edit_product", product_id=product_id)
            )

        record_audit_event(
            module_name="Database",
            event_type="product_updated",
            entity_type="product",
            entity_id=str(product.id),
            entity_name=product.sku_name,
            summary_text=f"Updated '{product.sku_name}' in the product master.",
            details={"vatable": bool(product.vatable), "uom": product.uom or "", "price": str(product.price or "")},
        )
        db.session.commit()
        flash(f"'{product.sku_name}' was updated.", "success")
        return redirect(url_for("product_master", q=search_query) if search_query else url_for("product_master"))

    @app.post("/products/<int:product_id>/deactivate")
    def deactivate_product(product_id: int) -> str:
        search_query = request.form.get("q", "").strip()
        try:
            product = set_product_active(product_id, False)
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for("product_master", q=search_query) if search_query else url_for("product_master"))

        record_audit_event(
            module_name="Database",
            event_type="product_deactivated",
            entity_type="product",
            entity_id=str(product.id),
            entity_name=product.sku_name,
            summary_text=f"Moved '{product.sku_name}' out of the active product master.",
        )
        db.session.commit()
        flash(f"'{product.sku_name}' was removed from the active product master.", "warning")
        return redirect(url_for("product_master", q=search_query) if search_query else url_for("product_master"))

    @app.post("/products/<int:product_id>/activate")
    def activate_product(product_id: int) -> str:
        search_query = request.form.get("q", "").strip()
        try:
            product = set_product_active(product_id, True)
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for("product_master", q=search_query) if search_query else url_for("product_master"))

        record_audit_event(
            module_name="Database",
            event_type="product_reactivated",
            entity_type="product",
            entity_id=str(product.id),
            entity_name=product.sku_name,
            summary_text=f"Restored '{product.sku_name}' into the active product master.",
        )
        db.session.commit()
        flash(f"'{product.sku_name}' is active again.", "success")
        return redirect(url_for("product_master", q=search_query) if search_query else url_for("product_master"))

    @app.post("/bp-rules")
    def create_brand_partner_rule() -> str:
        search_query = request.form.get("q", "").strip()
        return_to = request.form.get("return_to", "").strip()
        redirect_endpoint = "brand_partner_rules_home" if return_to == "bp_rules" else "product_master"
        try:
            rule = save_brand_partner_rule(dict(request.form))
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for(redirect_endpoint, q=search_query) if search_query else url_for(redirect_endpoint))

        record_audit_event(
            module_name="Database",
            event_type="bp_rule_created",
            entity_type="bp_rule",
            entity_id=str(rule.id),
            entity_name=rule.sku_name_pattern,
            summary_text=f"Added Brand Partner rule for '{rule.sku_name_pattern}'.",
            details={"store_name_pattern": rule.store_name_pattern or "", "rule_name": rule.rule_name or ""},
        )
        db.session.commit()
        flash(f"Brand Partner rule saved for '{rule.sku_name_pattern}'.", "success")
        return redirect(url_for(redirect_endpoint, q=search_query) if search_query else url_for(redirect_endpoint))

    @app.post("/bp-rules/<int:rule_id>/deactivate")
    def deactivate_brand_partner_rule(rule_id: int) -> str:
        search_query = request.form.get("q", "").strip()
        return_to = request.form.get("return_to", "").strip()
        redirect_endpoint = "brand_partner_rules_home" if return_to == "bp_rules" else "product_master"
        try:
            rule = set_brand_partner_rule_active(rule_id, False)
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for(redirect_endpoint, q=search_query) if search_query else url_for(redirect_endpoint))

        record_audit_event(
            module_name="Database",
            event_type="bp_rule_removed",
            entity_type="bp_rule",
            entity_id=str(rule.id),
            entity_name=rule.sku_name_pattern,
            summary_text=f"Removed Brand Partner rule for '{rule.sku_name_pattern}'.",
            details={"store_name_pattern": rule.store_name_pattern or "", "rule_name": rule.rule_name or ""},
        )
        db.session.commit()
        flash(f"Brand Partner rule for '{rule.sku_name_pattern}' was removed.", "warning")
        return redirect(url_for(redirect_endpoint, q=search_query) if search_query else url_for(redirect_endpoint))

    @app.post("/uom/import")
    def upload_uom() -> str:
        return_to = request.form.get("return_to", "").strip()
        redirect_target = "product_master" if return_to == "product_master" else "delivery_note_home"
        uploaded_file = request.files.get("uom_workbook")
        if uploaded_file is None or uploaded_file.filename == "":
            flash("Please choose the updated UOM workbook first.", "error")
            return redirect(url_for(redirect_target))

        try:
            outcome = import_uom_workbook(uploaded_file)
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for(redirect_target))

        if outcome.review is not None:
            flash(
                "UOM review ready. Please confirm the renamed or missing products before this source replaces the live catalog.",
                "warning",
            )
            return redirect(url_for("view_uom_review", review_id=outcome.review.id))

        import_log = outcome.import_log
        if import_log is None:
            flash("The UOM import did not produce a review or an applied source update.", "error")
            return redirect(url_for(redirect_target))

        skipped = getattr(import_log, "skipped_count", 0)
        deactivated = getattr(import_log, "deactivated_count", 0)
        record_audit_event(
            module_name="Database",
            event_type="uom_import_applied",
            entity_type="uom_import",
            entity_id=str(import_log.id),
            entity_name=import_log.filename,
            summary_text=f"Applied UOM source '{import_log.filename}' with {import_log.product_count} active rows.",
            details={
                "product_count": import_log.product_count,
                "skipped_count": skipped,
                "deactivated_count": deactivated,
            },
        )
        db.session.commit()
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
        return redirect(url_for(redirect_target))

    @app.post("/invoice-routing/import")
    def upload_invoice_routing() -> str:
        return_to = request.form.get("return_to", "").strip()
        redirect_target = "brand_partner_rules_home" if return_to == "bp_rules" else "product_master"
        uploaded_file = request.files.get("invoice_routing_workbook")
        if uploaded_file is None or uploaded_file.filename == "":
            flash("Please choose the invoice-routing database file first.", "error")
            return redirect(url_for(redirect_target))

        try:
            import_log = import_invoice_routing_workbook(uploaded_file)
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for(redirect_target))

        record_audit_event(
            module_name="Database",
            event_type="invoice_routing_imported",
            entity_type="invoice_routing_import",
            entity_id=str(import_log.id),
            entity_name=import_log.filename,
            summary_text=(
                f"Imported invoice-routing source '{import_log.filename}' with {import_log.row_count} live route rows."
            ),
            details={"row_count": import_log.row_count},
        )
        db.session.commit()
        flash(
            f"Invoice routing source updated. {import_log.row_count} store and SKU route rows now guide BP ownership.",
            "success",
        )
        return redirect(url_for(redirect_target))

    @app.get("/uom/reviews/<review_id>")
    def view_uom_review(review_id: str) -> str:
        review = get_uom_import_review(review_id)
        if review is None:
            abort(404)
        return render_template("uom_review.html", review=review)

    @app.post("/uom/reviews/<review_id>/apply")
    def submit_uom_review(review_id: str) -> str:
        review = get_uom_import_review(review_id)
        if review is None:
            abort(404)

        decisions = {
            str(item["product_id"]): request.form.get(f"decision::{item['product_id']}", "").strip()
            for item in (review.missing_products_json or [])
        }
        try:
            _, import_log = apply_uom_import_review(review_id, decisions)
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for("view_uom_review", review_id=review_id))

        flash(
            f"UOM review applied. {import_log.product_count} active rows now define the live source of truth.",
            "success",
        )
        return redirect(url_for("product_master"))

    @app.post("/uom/reviews/<review_id>/discard")
    def cancel_uom_review(review_id: str) -> str:
        try:
            discard_uom_import_review(review_id)
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for("view_uom_review", review_id=review_id))

        flash("The pending UOM review was dismissed. The live product master stayed unchanged.", "warning")
        return redirect(url_for("product_master"))

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
            record_audit_event(
                module_name="Delivery Note",
                event_type="run_created",
                entity_type="delivery_note_run",
                entity_id=run.id,
                entity_name=run.original_filename,
                summary_text=f"Imported Delivery Note tracker '{run.original_filename}' with {run.rows_detected} rows and {run.rows_needing_review} review item(s).",
                details={"rows_detected": run.rows_detected, "rows_ready": run.rows_ready, "rows_needing_review": run.rows_needing_review},
            )
            db.session.commit()
            flash("Tracker uploaded. Some SKUs need review before export.", "warning")
            return redirect(url_for("review_run", run_id=run.id))

        record_audit_event(
            module_name="Delivery Note",
            event_type="run_created",
            entity_type="delivery_note_run",
            entity_id=run.id,
            entity_name=run.original_filename,
            summary_text=f"Imported Delivery Note tracker '{run.original_filename}' with {run.rows_ready} ready rows.",
            details={"rows_detected": run.rows_detected, "rows_ready": run.rows_ready, "rows_needing_review": run.rows_needing_review},
        )
        db.session.commit()
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

        record_audit_event(
            module_name="Delivery Note",
            event_type="review_applied",
            entity_type="delivery_note_run",
            entity_id=run.id,
            entity_name=run.original_filename,
            summary_text=f"Completed Delivery Note review for '{run.original_filename}'. {run.rows_ready} row(s) are ready.",
            details={"rows_ready": run.rows_ready, "rows_needing_review": run.rows_needing_review},
        )
        db.session.commit()
        flash(f"Review saved. {run.rows_ready} rows are now ready for export.", "success")
        return redirect(url_for("view_run", run_id=run_id))

    @app.get("/runs/<run_id>/download")
    def download_run(run_id: str):
        invoice_category = request.args.get("category", "").strip() or None
        try:
            filename, payload = export_run_to_xls(run_id, invoice_category=invoice_category)
        except WorkbookShapeError as exc:
            flash(str(exc), "error")
            return redirect(url_for("view_run", run_id=run_id))

        record_audit_event(
            module_name="Delivery Note",
            event_type="exported",
            entity_type="delivery_note_run",
            entity_id=run_id,
            entity_name=filename,
            summary_text=f"Exported Delivery Note file '{filename}'.",
            details={"invoice_category": invoice_category or "all"},
        )
        db.session.commit()

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

        record_audit_event(
            module_name="Delivery Note",
            event_type="ignored_exported",
            entity_type="delivery_note_run",
            entity_id=run_id,
            entity_name=filename,
            summary_text=f"Downloaded ignored-item history for run {run_id}.",
        )
        db.session.commit()

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
        _ensure_loading_tracker_import_worker(app, active_import_job)
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
        active_import_job = get_active_loading_tracker_import_job()
        _ensure_loading_tracker_import_worker(app, active_import_job)
        return render_template(
            "loading_tracker_home.html",
            tracker_import=tracker_import,
            overview=overview,
            day_cards=tracker_import.days,
            template=template,
            template_context=build_loading_tracker_template_context(template),
            active_import_job=serialize_loading_tracker_import_job(active_import_job),
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
            _spawn_loading_tracker_import_worker(app, job_id)

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

        _ensure_loading_tracker_import_worker(app, job)
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

    @app.post("/loading-tracker/imports/<import_id>/days/<day_name>/bulk-move")
    def loading_tracker_day_bulk_move(import_id: str, day_name: str) -> str:
        target_day_name = request.form.get("target_day_name", "").strip() or PENDING_SENTINEL
        reason_code = request.form.get("reason_code", "").strip() or None
        reason_note = request.form.get("reason_note", "").strip() or None
        raw_row_ids = [value for value in request.form.getlist("row_ids") if value.strip()]
        try:
            rows = bulk_move_loading_tracker_rows(
                import_id,
                [int(value) for value in raw_row_ids],
                target_day_name,
                reason_code,
                reason_note,
            )
        except (LoadingTrackerError, ValueError) as exc:
            flash(str(exc), "error")
            return redirect(url_for("loading_tracker_day_view", import_id=import_id, day_name=day_name))

        if target_day_name == PENDING_SENTINEL:
            flash(f"{len(rows)} planner row(s) were moved into Pending.", "warning")
            return redirect(url_for("loading_tracker_pending_view", import_id=import_id))

        flash(f"{len(rows)} planner row(s) were moved into {target_day_name}.", "success")
        return redirect(url_for("loading_tracker_day_view", import_id=import_id, day_name=target_day_name))

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

    @app.post("/loading-tracker/imports/<import_id>/pending/bulk-move")
    def loading_tracker_pending_bulk_move(import_id: str) -> str:
        target_day_name = request.form.get("target_day_name", "").strip()
        raw_row_ids = [value for value in request.form.getlist("row_ids") if value.strip()]
        try:
            rows = bulk_move_loading_tracker_rows(
                import_id,
                [int(value) for value in raw_row_ids],
                target_day_name,
            )
        except (LoadingTrackerError, ValueError) as exc:
            flash(str(exc), "error")
            return redirect(url_for("loading_tracker_pending_view", import_id=import_id))

        flash(f"{len(rows)} pending row(s) were moved into {target_day_name}.", "success")
        return redirect(url_for("loading_tracker_day_view", import_id=import_id, day_name=target_day_name))

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

def _ensure_loading_tracker_import_worker(app: Flask, job) -> None:
    if app.config.get("TESTING") or app.config.get("LOADING_TRACKER_IMPORT_SYNC"):
        return
    if not _loading_tracker_job_needs_worker(job):
        return
    _spawn_loading_tracker_import_worker(app, job.id)


def _loading_tracker_job_needs_worker(job) -> bool:
    if job is None or job.tracker_import_id or job.status == "failed":
        return False
    if job.status == "queued":
        return True
    if job.status != "running" or job.updated_at is None:
        return False
    return job.updated_at < datetime.now(UTC) - timedelta(minutes=15)


def _spawn_loading_tracker_import_worker(app: Flask, job_id: str) -> None:
    worker_script = Path(__file__).resolve().with_name("loading_tracker_worker.py")
    if not worker_script.exists():
        return
    subprocess.Popen(
        [sys.executable, str(worker_script), job_id],
        cwd=str(Path(__file__).resolve().parent),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=os.environ.copy(),
    )


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
