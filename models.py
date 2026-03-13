from __future__ import annotations

from datetime import UTC, datetime

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import inspect, text

db = SQLAlchemy()


def utcnow() -> datetime:
    return datetime.now(UTC)


class UomImport(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    product_count = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)


class UomImportReview(db.Model):
    id = db.Column(db.String(32), primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(32), nullable=False, default="pending", index=True)
    row_count = db.Column(db.Integer, nullable=False, default=0)
    matched_count = db.Column(db.Integer, nullable=False, default=0)
    new_count = db.Column(db.Integer, nullable=False, default=0)
    missing_count = db.Column(db.Integer, nullable=False, default=0)
    rename_candidate_count = db.Column(db.Integer, nullable=False, default=0)
    rows_json = db.Column(db.JSON, nullable=False, default=list)
    unmatched_rows_json = db.Column(db.JSON, nullable=False, default=list)
    missing_products_json = db.Column(db.JSON, nullable=False, default=list)
    import_log_id = db.Column(db.Integer, db.ForeignKey("uom_import.id"), nullable=True, index=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    applied_at = db.Column(db.DateTime(timezone=True), nullable=True)


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


class BrandPartnerRule(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    rule_name = db.Column(db.String(255), nullable=True)
    sku_name_pattern = db.Column(db.String(255), nullable=False)
    normalized_sku_pattern = db.Column(db.String(255), nullable=False, index=True)
    store_name_pattern = db.Column(db.String(255), nullable=True)
    normalized_store_pattern = db.Column(db.String(255), nullable=True, index=True)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)


class InvoiceRoutingImport(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    row_count = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)

    entries = db.relationship("InvoiceRoutingEntry", back_populates="routing_import", cascade="all, delete-orphan")


class InvoiceRoutingEntry(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    import_id = db.Column(db.Integer, db.ForeignKey("invoice_routing_import.id"), nullable=False, index=True)
    brand_name = db.Column(db.String(255), nullable=True)
    normalized_brand_name = db.Column(db.String(255), nullable=True, index=True)
    sku_name = db.Column(db.String(255), nullable=False)
    normalized_sku_name = db.Column(db.String(255), nullable=False, index=True)
    party_name = db.Column(db.String(255), nullable=False)
    normalized_party_name = db.Column(db.String(255), nullable=False, index=True)
    invoice_name = db.Column(db.String(255), nullable=False)
    normalized_invoice_name = db.Column(db.String(255), nullable=False, index=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)

    routing_import = db.relationship("InvoiceRoutingImport", back_populates="entries")


class TallyBridgeProfile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    connection_mode = db.Column(db.String(32), nullable=False, default="manual_fallback")
    company_name = db.Column(db.String(255), nullable=True)
    tally_version = db.Column(db.String(255), nullable=True)
    endpoint_url = db.Column(db.String(255), nullable=True)
    machine_name = db.Column(db.String(255), nullable=True)
    notes = db.Column(db.Text, nullable=True)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    last_checked_at = db.Column(db.DateTime(timezone=True), nullable=True)
    capabilities_json = db.Column(db.JSON, nullable=False, default=dict)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    diagnostics_runs = db.relationship(
        "TallyDiagnosticsRun",
        back_populates="profile",
        order_by="TallyDiagnosticsRun.created_at.desc()",
    )


class TallyDiagnosticsRun(db.Model):
    id = db.Column(db.String(32), primary_key=True)
    profile_id = db.Column(db.Integer, db.ForeignKey("tally_bridge_profile.id"), nullable=True, index=True)
    title = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(32), nullable=False, default="draft", index=True)
    recommended_mode = db.Column(db.String(32), nullable=True)
    xml_http_supported = db.Column(db.String(16), nullable=False, default="unknown")
    outbound_import_supported = db.Column(db.String(16), nullable=False, default="unknown")
    register_fetch_supported = db.Column(db.String(16), nullable=False, default="unknown")
    dn_link_supported = db.Column(db.String(16), nullable=False, default="unknown")
    manual_case_status = db.Column(db.String(32), nullable=False, default="missing")
    uploaded_case_status = db.Column(db.String(32), nullable=False, default="missing")
    findings_summary = db.Column(db.Text, nullable=True)
    notes = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)
    completed_at = db.Column(db.DateTime(timezone=True), nullable=True)

    profile = db.relationship("TallyBridgeProfile", back_populates="diagnostics_runs")
    artifacts = db.relationship(
        "TallyDiagnosticsArtifact",
        back_populates="diagnostics_run",
        cascade="all, delete-orphan",
        order_by="TallyDiagnosticsArtifact.created_at.desc(), TallyDiagnosticsArtifact.id.desc()",
    )


class TallyDiagnosticsArtifact(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    run_id = db.Column(db.String(32), db.ForeignKey("tally_diagnostics_run.id"), nullable=False, index=True)
    artifact_group = db.Column(db.String(32), nullable=False, default="other")
    artifact_type = db.Column(db.String(64), nullable=False)
    filename = db.Column(db.String(255), nullable=False)
    content_type = db.Column(db.String(255), nullable=True)
    storage_path = db.Column(db.String(512), nullable=False)
    description = db.Column(db.String(255), nullable=True)
    file_size = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)

    diagnostics_run = db.relationship("TallyDiagnosticsRun", back_populates="artifacts")


class TallyBridgeRun(db.Model):
    id = db.Column(db.String(32), primary_key=True)
    profile_id = db.Column(db.Integer, db.ForeignKey("tally_bridge_profile.id"), nullable=True, index=True)
    sales_order_run_id = db.Column(db.String(32), db.ForeignKey("sales_order_run.id"), nullable=False, index=True)
    status = db.Column(db.String(32), nullable=False, default="ready_to_send", index=True)
    bridge_mode = db.Column(db.String(32), nullable=False, default="manual_fallback")
    payload_filename = db.Column(db.String(255), nullable=False)
    payload_storage_path = db.Column(db.String(512), nullable=False)
    payload_content_type = db.Column(db.String(255), nullable=True)
    staged_storage_path = db.Column(db.String(512), nullable=True)
    rows_ready = db.Column(db.Integer, nullable=False, default=0)
    error_message = db.Column(db.Text, nullable=True)
    notes = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)
    sent_at = db.Column(db.DateTime(timezone=True), nullable=True)
    confirmed_at = db.Column(db.DateTime(timezone=True), nullable=True)

    profile = db.relationship("TallyBridgeProfile")
    sales_order_run = db.relationship("SalesOrderRun")


class AuditEvent(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    module_name = db.Column(db.String(64), nullable=False, index=True)
    event_type = db.Column(db.String(64), nullable=False, index=True)
    entity_type = db.Column(db.String(64), nullable=False)
    entity_id = db.Column(db.String(64), nullable=True)
    entity_name = db.Column(db.String(255), nullable=False)
    summary_text = db.Column(db.String(255), nullable=False)
    details_json = db.Column(db.JSON, nullable=False, default=dict)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, index=True)


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
    raw_reference_no = db.Column(db.String(128), nullable=True)
    invoice_owner = db.Column(db.String(8), nullable=True, index=True)
    tax_bucket = db.Column(db.String(8), nullable=True, index=True)
    invoice_route_name = db.Column(db.String(255), nullable=True)
    invoice_category = db.Column(db.String(8), nullable=True, index=True)
    prefixed_reference_no = db.Column(db.String(160), nullable=True)
    classification_source = db.Column(db.String(64), nullable=True)
    bp_rule_reason = db.Column(db.String(255), nullable=True)

    run = db.relationship("UploadRun", back_populates="lines")
    product = db.relationship("Product")


class SalesOrderRun(db.Model):
    id = db.Column(db.String(32), primary_key=True)
    original_filename = db.Column(db.String(255), nullable=False)
    source_sheet_name = db.Column(db.String(255), nullable=True)
    status = db.Column(db.String(32), nullable=False, default="needs_review")
    row_count = db.Column(db.Integer, nullable=False, default=0)
    rows_ready = db.Column(db.Integer, nullable=False, default=0)
    rows_needing_review = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    exported_at = db.Column(db.DateTime(timezone=True), nullable=True)

    lines = db.relationship("SalesOrderLine", back_populates="run", cascade="all, delete-orphan")


class SalesOrderLine(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    run_id = db.Column(db.String(32), db.ForeignKey("sales_order_run.id"), nullable=False, index=True)
    invoice_date = db.Column(db.String(32), nullable=False)
    order_number = db.Column(db.String(128), nullable=False)
    retailer_name = db.Column(db.String(255), nullable=False)
    source_sku = db.Column(db.String(255), nullable=False, index=True)
    normalized_source_sku = db.Column(db.String(255), nullable=False, index=True)
    source_uom = db.Column(db.String(64), nullable=True)
    source_quantity = db.Column(db.Numeric(14, 4), nullable=False)
    source_rate = db.Column(db.Numeric(14, 4), nullable=False)
    source_amount = db.Column(db.Numeric(14, 4), nullable=False)
    status = db.Column(db.String(32), nullable=False, default="needs_review")
    matched_by = db.Column(db.String(64), nullable=True)
    product_id = db.Column(db.Integer, db.ForeignKey("product.id"), nullable=True)
    resolved_sku_name = db.Column(db.String(255), nullable=True)
    resolved_uom = db.Column(db.String(64), nullable=True)
    resolved_quantity = db.Column(db.Numeric(14, 4), nullable=True)
    resolved_quantity_text = db.Column(db.String(64), nullable=True)
    resolved_rate = db.Column(db.Numeric(14, 4), nullable=True)
    resolved_amount = db.Column(db.Numeric(14, 4), nullable=True)
    resolved_vatable = db.Column(db.Boolean, nullable=False, default=False)
    raw_reference_no = db.Column(db.String(128), nullable=True)
    invoice_owner = db.Column(db.String(8), nullable=True, index=True)
    tax_bucket = db.Column(db.String(8), nullable=True, index=True)
    invoice_route_name = db.Column(db.String(255), nullable=True)
    invoice_category = db.Column(db.String(8), nullable=True, index=True)
    prefixed_reference_no = db.Column(db.String(160), nullable=True)
    classification_source = db.Column(db.String(64), nullable=True)
    bp_rule_reason = db.Column(db.String(255), nullable=True)

    run = db.relationship("SalesOrderRun", back_populates="lines")
    product = db.relationship("Product")


class SkuAutomatorRun(db.Model):
    id = db.Column(db.String(32), primary_key=True)
    original_filename = db.Column(db.String(255), nullable=False)
    source_sheet_name = db.Column(db.String(255), nullable=True)
    status = db.Column(db.String(32), nullable=False, default="needs_review")
    voucher_count = db.Column(db.Integer, nullable=False, default=0)
    order_reference_count = db.Column(db.Integer, nullable=False, default=0)
    line_count = db.Column(db.Integer, nullable=False, default=0)
    rows_ready = db.Column(db.Integer, nullable=False, default=0)
    rows_needing_review = db.Column(db.Integer, nullable=False, default=0)
    store_count = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    exported_at = db.Column(db.DateTime(timezone=True), nullable=True)

    lines = db.relationship("SkuAutomatorLine", back_populates="run", cascade="all, delete-orphan")


class SkuAutomatorLine(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    run_id = db.Column(db.String(32), db.ForeignKey("sku_automator_run.id"), nullable=False, index=True)
    order_date = db.Column(db.String(32), nullable=False)
    store_name = db.Column(db.String(255), nullable=False)
    voucher_no = db.Column(db.String(128), nullable=True)
    order_reference_no = db.Column(db.String(128), nullable=True)
    source_sku = db.Column(db.String(255), nullable=False, index=True)
    normalized_source_sku = db.Column(db.String(255), nullable=False, index=True)
    source_value = db.Column(db.Numeric(14, 4), nullable=False)
    status = db.Column(db.String(32), nullable=False, default="needs_review")
    matched_by = db.Column(db.String(64), nullable=True)
    product_id = db.Column(db.Integer, db.ForeignKey("product.id"), nullable=True)
    resolved_sku_name = db.Column(db.String(255), nullable=True)
    resolved_quantity = db.Column(db.Numeric(14, 4), nullable=True)
    resolved_rate = db.Column(db.Numeric(14, 4), nullable=True)
    raw_reference_no = db.Column(db.String(128), nullable=True)
    invoice_owner = db.Column(db.String(8), nullable=True, index=True)
    tax_bucket = db.Column(db.String(8), nullable=True, index=True)
    invoice_route_name = db.Column(db.String(255), nullable=True)
    invoice_category = db.Column(db.String(8), nullable=True, index=True)
    prefixed_reference_no = db.Column(db.String(160), nullable=True)
    classification_source = db.Column(db.String(64), nullable=True)
    bp_rule_reason = db.Column(db.String(255), nullable=True)

    run = db.relationship("SkuAutomatorRun", back_populates="lines")
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
    planning_rows = db.relationship(
        "LoadingTrackerRow",
        back_populates="tracker_import",
        cascade="all, delete-orphan",
        order_by="LoadingTrackerRow.sort_order.asc(), LoadingTrackerRow.id.asc()",
    )
    inventory_items = db.relationship(
        "LoadingTrackerInventoryItem",
        back_populates="tracker_import",
        cascade="all, delete-orphan",
        order_by="LoadingTrackerInventoryItem.sort_order.asc(), LoadingTrackerInventoryItem.id.asc()",
    )
    fee_items = db.relationship(
        "LoadingTrackerFeeItem",
        back_populates="tracker_import",
        cascade="all, delete-orphan",
        order_by="LoadingTrackerFeeItem.id.asc()",
    )
    events = db.relationship(
        "LoadingTrackerEvent",
        back_populates="tracker_import",
        cascade="all, delete-orphan",
        order_by="LoadingTrackerEvent.created_at.desc(), LoadingTrackerEvent.id.desc()",
    )
    import_jobs = db.relationship(
        "LoadingTrackerImportJob",
        back_populates="tracker_import",
        order_by="LoadingTrackerImportJob.created_at.desc()",
    )


class LoadingTrackerImportJob(db.Model):
    id = db.Column(db.String(32), primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    status = db.Column(db.String(32), nullable=False, default="queued", index=True)
    progress_percent = db.Column(db.Integer, nullable=False, default=0)
    stage_label = db.Column(db.String(255), nullable=True)
    error_message = db.Column(db.Text, nullable=True)
    tracker_import_id = db.Column(
        db.String(32), db.ForeignKey("loading_tracker_import.id"), nullable=True, index=True
    )
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    tracker_import = db.relationship("LoadingTrackerImport", back_populates="import_jobs")


class LoadingTrackerTemplate(db.Model):
    id = db.Column(db.String(32), primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text, nullable=True)
    is_active = db.Column(db.Boolean, nullable=False, default=True, index=True)
    source_import_label = db.Column(db.String(255), nullable=True)
    assumptions_sku_count = db.Column(db.Integer, nullable=False, default=0)
    assumptions_store_count = db.Column(db.Integer, nullable=False, default=0)
    fees_row_count = db.Column(db.Integer, nullable=False, default=0)
    notes_count = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    days = db.relationship(
        "LoadingTrackerTemplateDay",
        back_populates="template",
        cascade="all, delete-orphan",
        order_by="LoadingTrackerTemplateDay.day_order.asc()",
    )
    rows = db.relationship(
        "LoadingTrackerTemplateRow",
        back_populates="template",
        cascade="all, delete-orphan",
        order_by="LoadingTrackerTemplateRow.sort_order.asc(), LoadingTrackerTemplateRow.id.asc()",
    )
    inventory_items = db.relationship(
        "LoadingTrackerTemplateInventoryItem",
        back_populates="template",
        cascade="all, delete-orphan",
        order_by="LoadingTrackerTemplateInventoryItem.sort_order.asc(), LoadingTrackerTemplateInventoryItem.id.asc()",
    )
    fee_items = db.relationship(
        "LoadingTrackerTemplateFeeItem",
        back_populates="template",
        cascade="all, delete-orphan",
        order_by="LoadingTrackerTemplateFeeItem.id.asc()",
    )


class LoadingTrackerTemplateDay(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    template_id = db.Column(db.String(32), db.ForeignKey("loading_tracker_template.id"), nullable=False, index=True)
    day_name = db.Column(db.String(32), nullable=False)
    day_order = db.Column(db.Integer, nullable=False, default=0)

    template = db.relationship("LoadingTrackerTemplate", back_populates="days")
    rows = db.relationship(
        "LoadingTrackerTemplateRow",
        back_populates="day",
        cascade="all, delete-orphan",
        order_by="LoadingTrackerTemplateRow.sort_order.asc(), LoadingTrackerTemplateRow.id.asc()",
    )


class LoadingTrackerTemplateRow(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    template_id = db.Column(db.String(32), db.ForeignKey("loading_tracker_template.id"), nullable=False, index=True)
    day_id = db.Column(db.Integer, db.ForeignKey("loading_tracker_template_day.id"), nullable=True, index=True)
    row_state = db.Column(db.String(32), nullable=False, default="planned", index=True)
    batch_name = db.Column(db.String(64), nullable=False, default="Load 1")
    store_name = db.Column(db.String(255), nullable=False)
    contact = db.Column(db.String(255), nullable=True)
    lp = db.Column(db.String(255), nullable=True)
    tier = db.Column(db.String(255), nullable=True)
    region = db.Column(db.String(255), nullable=True)
    delivery_date = db.Column(db.String(64), nullable=True)
    reason_text = db.Column(db.String(255), nullable=True)
    total_weight = db.Column(db.Numeric(14, 4), nullable=True)
    total_value = db.Column(db.Numeric(14, 4), nullable=True)
    sort_order = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    template = db.relationship("LoadingTrackerTemplate", back_populates="rows")
    day = db.relationship("LoadingTrackerTemplateDay", back_populates="rows")
    items = db.relationship(
        "LoadingTrackerTemplateRowItem",
        back_populates="row",
        cascade="all, delete-orphan",
        order_by="LoadingTrackerTemplateRowItem.id.asc()",
    )


class LoadingTrackerTemplateRowItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    row_id = db.Column(db.Integer, db.ForeignKey("loading_tracker_template_row.id"), nullable=False, index=True)
    sku_name = db.Column(db.String(255), nullable=False)
    quantity = db.Column(db.Numeric(14, 4), nullable=False, default=0)

    row = db.relationship("LoadingTrackerTemplateRow", back_populates="items")


class LoadingTrackerTemplateInventoryItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    template_id = db.Column(db.String(32), db.ForeignKey("loading_tracker_template.id"), nullable=False, index=True)
    sku_name = db.Column(db.String(255), nullable=False)
    sort_order = db.Column(db.Integer, nullable=False, default=0)

    template = db.relationship("LoadingTrackerTemplate", back_populates="inventory_items")


class LoadingTrackerTemplateFeeItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    template_id = db.Column(db.String(32), db.ForeignKey("loading_tracker_template.id"), nullable=False, index=True)
    brand_partner = db.Column(db.String(255), nullable=True)
    sku_name = db.Column(db.String(255), nullable=False)
    vatable_text = db.Column(db.String(64), nullable=True)
    retail_delivery_value = db.Column(db.Numeric(14, 4), nullable=True)
    payment_collection_value = db.Column(db.Numeric(14, 4), nullable=True)

    template = db.relationship("LoadingTrackerTemplate", back_populates="fee_items")


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
    planning_rows = db.relationship(
        "LoadingTrackerRow",
        back_populates="day",
        cascade="all, delete-orphan",
        order_by="LoadingTrackerRow.sort_order.asc(), LoadingTrackerRow.id.asc()",
    )
    inventory_counts = db.relationship(
        "LoadingTrackerDailyCount",
        back_populates="day",
        cascade="all, delete-orphan",
        order_by="LoadingTrackerDailyCount.sku_name.asc()",
    )
    events = db.relationship(
        "LoadingTrackerEvent",
        back_populates="day",
        order_by="LoadingTrackerEvent.created_at.desc(), LoadingTrackerEvent.id.desc()",
    )


class LoadingTrackerRow(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tracker_import_id = db.Column(db.String(32), db.ForeignKey("loading_tracker_import.id"), nullable=False, index=True)
    day_id = db.Column(db.Integer, db.ForeignKey("loading_tracker_day.id"), nullable=True, index=True)
    row_state = db.Column(db.String(32), nullable=False, default="planned", index=True)
    source_kind = db.Column(db.String(32), nullable=False, default="import")
    batch_name = db.Column(db.String(64), nullable=False, default="Load 1")
    store_name = db.Column(db.String(255), nullable=False)
    contact = db.Column(db.String(255), nullable=True)
    lp = db.Column(db.String(255), nullable=True)
    tier = db.Column(db.String(255), nullable=True)
    region = db.Column(db.String(255), nullable=True)
    delivery_date = db.Column(db.String(64), nullable=True)
    reason_text = db.Column(db.String(255), nullable=True)
    total_weight = db.Column(db.Numeric(14, 4), nullable=True)
    total_value = db.Column(db.Numeric(14, 4), nullable=True)
    sort_order = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    tracker_import = db.relationship("LoadingTrackerImport", back_populates="planning_rows")
    day = db.relationship("LoadingTrackerDay", back_populates="planning_rows")
    items = db.relationship(
        "LoadingTrackerRowItem",
        back_populates="row",
        cascade="all, delete-orphan",
        order_by="LoadingTrackerRowItem.id.asc()",
    )
    events = db.relationship(
        "LoadingTrackerEvent",
        back_populates="row",
        order_by="LoadingTrackerEvent.created_at.desc(), LoadingTrackerEvent.id.desc()",
    )


class LoadingTrackerRowItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    row_id = db.Column(db.Integer, db.ForeignKey("loading_tracker_row.id"), nullable=False, index=True)
    sku_name = db.Column(db.String(255), nullable=False)
    quantity = db.Column(db.Numeric(14, 4), nullable=False, default=0)
    raw_reference_no = db.Column(db.String(128), nullable=True)
    invoice_owner = db.Column(db.String(8), nullable=True, index=True)
    tax_bucket = db.Column(db.String(8), nullable=True, index=True)
    invoice_route_name = db.Column(db.String(255), nullable=True)
    invoice_category = db.Column(db.String(8), nullable=True, index=True)
    prefixed_reference_no = db.Column(db.String(160), nullable=True)
    classification_source = db.Column(db.String(64), nullable=True)
    bp_rule_reason = db.Column(db.String(255), nullable=True)

    row = db.relationship("LoadingTrackerRow", back_populates="items")


RUNTIME_SCHEMA_UPDATES = {
    "upload_line": {
        "raw_reference_no": "VARCHAR(128)",
        "invoice_owner": "VARCHAR(8)",
        "tax_bucket": "VARCHAR(8)",
        "invoice_route_name": "VARCHAR(255)",
        "invoice_category": "VARCHAR(8)",
        "prefixed_reference_no": "VARCHAR(160)",
        "classification_source": "VARCHAR(64)",
        "bp_rule_reason": "VARCHAR(255)",
    },
    "sales_order_line": {
        "raw_reference_no": "VARCHAR(128)",
        "invoice_owner": "VARCHAR(8)",
        "tax_bucket": "VARCHAR(8)",
        "invoice_route_name": "VARCHAR(255)",
        "invoice_category": "VARCHAR(8)",
        "prefixed_reference_no": "VARCHAR(160)",
        "classification_source": "VARCHAR(64)",
        "bp_rule_reason": "VARCHAR(255)",
    },
    "sku_automator_line": {
        "raw_reference_no": "VARCHAR(128)",
        "invoice_owner": "VARCHAR(8)",
        "tax_bucket": "VARCHAR(8)",
        "invoice_route_name": "VARCHAR(255)",
        "invoice_category": "VARCHAR(8)",
        "prefixed_reference_no": "VARCHAR(160)",
        "classification_source": "VARCHAR(64)",
        "bp_rule_reason": "VARCHAR(255)",
    },
    "loading_tracker_row_item": {
        "raw_reference_no": "VARCHAR(128)",
        "invoice_owner": "VARCHAR(8)",
        "tax_bucket": "VARCHAR(8)",
        "invoice_route_name": "VARCHAR(255)",
        "invoice_category": "VARCHAR(8)",
        "prefixed_reference_no": "VARCHAR(160)",
        "classification_source": "VARCHAR(64)",
        "bp_rule_reason": "VARCHAR(255)",
    },
}


def ensure_runtime_schema(engine) -> None:
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    with engine.begin() as connection:
        for table_name, columns in RUNTIME_SCHEMA_UPDATES.items():
            if table_name not in existing_tables:
                continue
            existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
            for column_name, column_type in columns.items():
                if column_name in existing_columns:
                    continue
                connection.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN "{column_name}" {column_type}'))


class LoadingTrackerInventoryItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tracker_import_id = db.Column(db.String(32), db.ForeignKey("loading_tracker_import.id"), nullable=False, index=True)
    sku_name = db.Column(db.String(255), nullable=False)
    opening_g2g_qty = db.Column(db.Numeric(14, 4), nullable=True)
    opening_remaining_qty = db.Column(db.Numeric(14, 4), nullable=True)
    added_qty = db.Column(db.Numeric(14, 4), nullable=False, default=0)
    sort_order = db.Column(db.Integer, nullable=False, default=0)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    tracker_import = db.relationship("LoadingTrackerImport", back_populates="inventory_items")


class LoadingTrackerFeeItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tracker_import_id = db.Column(db.String(32), db.ForeignKey("loading_tracker_import.id"), nullable=False, index=True)
    brand_partner = db.Column(db.String(255), nullable=True)
    sku_name = db.Column(db.String(255), nullable=False)
    vatable_text = db.Column(db.String(64), nullable=True)
    retail_delivery_value = db.Column(db.Numeric(14, 4), nullable=True)
    payment_collection_value = db.Column(db.Numeric(14, 4), nullable=True)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)

    tracker_import = db.relationship("LoadingTrackerImport", back_populates="fee_items")


class LoadingTrackerDailyCount(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    day_id = db.Column(db.Integer, db.ForeignKey("loading_tracker_day.id"), nullable=False, index=True)
    sku_name = db.Column(db.String(255), nullable=False, index=True)
    expected_qty = db.Column(db.Numeric(14, 4), nullable=False, default=0)
    physical_qty = db.Column(db.Numeric(14, 4), nullable=False, default=0)
    discrepancy_qty = db.Column(db.Numeric(14, 4), nullable=False, default=0)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow)

    day = db.relationship("LoadingTrackerDay", back_populates="inventory_counts")


class LoadingTrackerEvent(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tracker_import_id = db.Column(db.String(32), db.ForeignKey("loading_tracker_import.id"), nullable=False, index=True)
    day_id = db.Column(db.Integer, db.ForeignKey("loading_tracker_day.id"), nullable=True, index=True)
    row_id = db.Column(db.Integer, db.ForeignKey("loading_tracker_row.id"), nullable=True, index=True)
    event_type = db.Column(db.String(64), nullable=False, index=True)
    entity_name = db.Column(db.String(255), nullable=False)
    reason_code = db.Column(db.String(64), nullable=True)
    reason_text = db.Column(db.String(255), nullable=True)
    details_json = db.Column(db.JSON, nullable=False, default=dict)
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utcnow)

    tracker_import = db.relationship("LoadingTrackerImport", back_populates="events")
    day = db.relationship("LoadingTrackerDay", back_populates="events")
    row = db.relationship("LoadingTrackerRow", back_populates="events")
