from __future__ import annotations

import os
import secrets
from io import BytesIO
from pathlib import Path

from flask import Flask, abort, flash, redirect, render_template, request, send_file, url_for

from models import Product, db
from services import (
    ServiceError,
    WorkbookShapeError,
    apply_review_decisions,
    build_dashboard_summary,
    build_run_summary,
    create_tracker_run,
    export_run_to_xls,
    import_uom_workbook,
    save_product_master_entry,
    set_product_active,
)

APP_TIMEZONE = os.environ.get("APP_TIMEZONE", "Africa/Lagos")


def create_app(test_config: dict | None = None) -> Flask:
    app = Flask(__name__, instance_relative_config=True)
    Path(app.instance_path).mkdir(parents=True, exist_ok=True)

    app.config.update(
        SECRET_KEY=os.environ.get("SECRET_KEY", secrets.token_hex(24)),
        APP_TIMEZONE=APP_TIMEZONE,
        SQLALCHEMY_DATABASE_URI=_database_uri(app.instance_path),
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )
    if test_config:
        app.config.update(test_config)

    db.init_app(app)
    with app.app_context():
        db.create_all()

    @app.get("/")
    def index() -> str:
        summary = build_dashboard_summary()
        return render_template("dashboard.html", summary=summary)

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
            return redirect(url_for("index"))

        try:
            import_log = import_uom_workbook(uploaded_file)
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for("index"))

        flash(f"UOM import complete. {import_log.product_count} product rows were saved.", "success")
        return redirect(url_for("index"))

    @app.post("/runs/import")
    def upload_tracker() -> str:
        uploaded_file = request.files.get("tracker_workbook")
        if uploaded_file is None or uploaded_file.filename == "":
            flash("Please upload the loading tracker workbook first.", "error")
            return redirect(url_for("index"))

        try:
            run = create_tracker_run(uploaded_file, app.config["APP_TIMEZONE"])
        except ServiceError as exc:
            flash(str(exc), "error")
            return redirect(url_for("index"))

        if run.rows_detected == 0:
            flash("No rows above 0.00 were found in the tracker file.", "error")
            return redirect(url_for("index"))

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
        return render_template("run_detail.html", summary=summary)

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

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


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
