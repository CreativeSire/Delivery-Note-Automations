from __future__ import annotations

from datetime import UTC, datetime

from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


def utcnow() -> datetime:
    return datetime.now(UTC)


class UomImport(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    product_count = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)


class Product(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    sku_name = db.Column(db.String(255), nullable=False, unique=True)
    normalized_name = db.Column(db.String(255), nullable=False, index=True)
    uom = db.Column(db.String(64), nullable=True)
    alt_uom = db.Column(db.String(64), nullable=True)
    conversion = db.Column(db.Numeric(12, 4), nullable=True)
    price = db.Column(db.Numeric(14, 4), nullable=True)
    vatable = db.Column(db.Boolean, nullable=False, default=False)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    source_import_id = db.Column(db.Integer, db.ForeignKey("uom_import.id"), nullable=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    aliases = db.relationship("ProductAlias", back_populates="product", cascade="all, delete-orphan")


class ProductAlias(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    alias_name = db.Column(db.String(255), nullable=False, unique=True)
    normalized_name = db.Column(db.String(255), nullable=False, index=True)
    match_method = db.Column(db.String(64), nullable=False, default="manual")
    product_id = db.Column(db.Integer, db.ForeignKey("product.id"), nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)

    product = db.relationship("Product", back_populates="aliases")


class UploadRun(db.Model):
    id = db.Column(db.String(32), primary_key=True)
    original_filename = db.Column(db.String(255), nullable=False)
    invoice_date = db.Column(db.String(32), nullable=False)
    status = db.Column(db.String(32), nullable=False, default="needs_review")
    rows_detected = db.Column(db.Integer, nullable=False, default=0)
    rows_ready = db.Column(db.Integer, nullable=False, default=0)
    rows_needing_review = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    exported_at = db.Column(db.DateTime(timezone=True), nullable=True)

    lines = db.relationship("UploadLine", back_populates="run", cascade="all, delete-orphan")


class UploadLine(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    run_id = db.Column(db.String(32), db.ForeignKey("upload_run.id"), nullable=False, index=True)
    order_number = db.Column(db.String(128), nullable=False)
    supermarket_name = db.Column(db.String(255), nullable=False)
    source_sku = db.Column(db.String(255), nullable=False, index=True)
    normalized_source_sku = db.Column(db.String(255), nullable=False, index=True)
    quantity = db.Column(db.Numeric(14, 4), nullable=False)
    status = db.Column(db.String(32), nullable=False, default="needs_review")
    matched_by = db.Column(db.String(64), nullable=True)
    product_id = db.Column(db.Integer, db.ForeignKey("product.id"), nullable=True)
    resolved_sku_name = db.Column(db.String(255), nullable=True)
    resolved_rate = db.Column(db.Numeric(14, 4), nullable=True)
    resolved_vatable = db.Column(db.Boolean, nullable=False, default=False)

    run = db.relationship("UploadRun", back_populates="lines")
    product = db.relationship("Product")


class LoadingTrackerImport(db.Model):
    id = db.Column(db.String(32), primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    week_label = db.Column(db.String(255), nullable=False)
    assumptions_sku_count = db.Column(db.Integer, nullable=False, default=0)
    assumptions_store_count = db.Column(db.Integer, nullable=False, default=0)
    pending_g2g_total = db.Column(db.Numeric(14, 4), nullable=True)
    pending_loaded_total = db.Column(db.Numeric(14, 4), nullable=True)
    pending_remaining_total = db.Column(db.Numeric(14, 4), nullable=True)
    opening_g2g_total = db.Column(db.Numeric(14, 4), nullable=True)
    opening_remaining_total = db.Column(db.Numeric(14, 4), nullable=True)
    fees_row_count = db.Column(db.Integer, nullable=False, default=0)
    fees_total_delivery_value = db.Column(db.Numeric(14, 4), nullable=True)
    fees_total_payment_value = db.Column(db.Numeric(14, 4), nullable=True)
    notes_count = db.Column(db.Integer, nullable=False, default=0)
    pending_rows_json = db.Column(db.JSON, nullable=False, default=list)
    pending_top_products_json = db.Column(db.JSON, nullable=False, default=list)
    opening_top_products_json = db.Column(db.JSON, nullable=False, default=list)
    fee_rows_json = db.Column(db.JSON, nullable=False, default=list)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)

    days = db.relationship(
        "LoadingTrackerDay",
        back_populates="tracker_import",
        cascade="all, delete-orphan",
        order_by="LoadingTrackerDay.day_order.asc()",
    )


class LoadingTrackerDay(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tracker_import_id = db.Column(db.String(32), db.ForeignKey("loading_tracker_import.id"), nullable=False, index=True)
    day_name = db.Column(db.String(32), nullable=False)
    day_order = db.Column(db.Integer, nullable=False, default=0)
    g2g_total = db.Column(db.Numeric(14, 4), nullable=True)
    loaded_total = db.Column(db.Numeric(14, 4), nullable=True)
    remaining_total = db.Column(db.Numeric(14, 4), nullable=True)
    expected_store_total = db.Column(db.Numeric(14, 4), nullable=True)
    batch_count = db.Column(db.Integer, nullable=False, default=0)
    active_store_count = db.Column(db.Integer, nullable=False, default=0)
    total_weight = db.Column(db.Numeric(14, 4), nullable=True)
    total_value = db.Column(db.Numeric(14, 4), nullable=True)
    load_1_total = db.Column(db.Numeric(14, 4), nullable=True)
    load_2_total = db.Column(db.Numeric(14, 4), nullable=True)
    load_3_total = db.Column(db.Numeric(14, 4), nullable=True)
    load_4_total = db.Column(db.Numeric(14, 4), nullable=True)
    load_total = db.Column(db.Numeric(14, 4), nullable=True)
    store_rows_json = db.Column(db.JSON, nullable=False, default=list)
    top_products_json = db.Column(db.JSON, nullable=False, default=list)
    load_rows_json = db.Column(db.JSON, nullable=False, default=list)

    tracker_import = db.relationship("LoadingTrackerImport", back_populates="days")
