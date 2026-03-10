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
