from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import xlwt
from flask import current_app
from openpyxl import load_workbook
from sqlalchemy import func, select

from models import Product, ProductAlias, UploadLine, UploadRun, UomImport, db

TRACKER_SHEET = "tracker"
UOM_SHEET = "UOM"
TEMPLATE_SHEET = "Delivery Invoice"
DATE_FORMAT = "%Y-%m-%d"
TRACKER_ORDER_HEADERS = {"sales order number", "order number"}
TRACKER_STORE_HEADERS = {"stores", "store", "supermarket", "supermarket name"}
TRACKER_MIN_COLUMNS = 8
OUTPUT_HEADERS = [
    "Invoice Date",
    "Order Number",
    "Voucher Type Name",
    "Party name",
    "stock Item Name",
    "Quantity",
    "Rate",
    "Amount",
    "SalesLedger Name",
    "VAT",
    "VAT%",
    "VAT Amount",
]
VOUCHER_TYPE_NAME = "Delivery Invoice"
SALES_LEDGER_NAME = "Inventory Pool"
VAT_LABEL = "VAT"
VAT_RATE = Decimal("7.5")
SEED_UOM_PATH = Path(__file__).resolve().parent / "data" / "latest_uom_seed.csv"
INVALID_FILENAME_CHARS = r'[<>:"/\\|?*\x00-\x1f]'


class ServiceError(Exception):
    pass


class WorkbookShapeError(ServiceError):
    pass


@dataclass
class DashboardSummary:
    product_count: int
    inactive_product_count: int
    alias_count: int
    import_count: int
    run_count: int
    latest_import: UomImport | None
    recent_imports: list[UomImport]
    recent_runs: list[UploadRun]


@dataclass
class ProductMatch:
    product: Product
    match_method: str


@dataclass
class UnresolvedGroup:
    source_sku: str
    occurrences: int
    order_numbers: list[str]
    supermarkets: list[str]
    suggestions: list[Product]


@dataclass
class IgnoredGroup:
    source_sku: str
    occurrences: int
    order_numbers: list[str]
    supermarkets: list[str]
    reason: str


@dataclass
class RunSummary:
    run: UploadRun
    unresolved_groups: list[UnresolvedGroup]
    ignored_groups: list[IgnoredGroup]
    ready_lines: int
    ignored_lines: int


def build_dashboard_summary() -> DashboardSummary:
    latest_import = db.session.scalar(select(UomImport).order_by(UomImport.created_at.desc()).limit(1))
    return DashboardSummary(
        product_count=db.session.scalar(select(func.count(Product.id)).where(Product.is_active.is_(True))) or 0,
        inactive_product_count=db.session.scalar(select(func.count(Product.id)).where(Product.is_active.is_(False))) or 0,
        alias_count=db.session.scalar(select(func.count(ProductAlias.id))) or 0,
        import_count=db.session.scalar(select(func.count(UomImport.id))) or 0,
        run_count=db.session.scalar(select(func.count(UploadRun.id))) or 0,
        latest_import=latest_import,
        recent_imports=list(db.session.scalars(select(UomImport).order_by(UomImport.created_at.desc()).limit(5))),
        recent_runs=list(db.session.scalars(select(UploadRun).order_by(UploadRun.created_at.desc()).limit(8))),
    )


def import_uom_workbook(file_storage: Any) -> UomImport:
    payload = _read_upload_payload(file_storage)
    filename = file_storage.filename or "uom.xlsx"

    workbook = _try_load_workbook(payload)
    if workbook is not None:
        if UOM_SHEET not in workbook.sheetnames:
            raise WorkbookShapeError(f"The workbook must contain a '{UOM_SHEET}' sheet.")

        sheet = workbook[UOM_SHEET]
        rows = [
            [
                sheet.cell(row_index, 1).value,
                sheet.cell(row_index, 2).value,
                sheet.cell(row_index, 3).value,
                sheet.cell(row_index, 4).value,
                sheet.cell(row_index, 5).value,
                sheet.cell(row_index, 6).value,
            ]
            for row_index in range(2, sheet.max_row + 1)
        ]
        return import_uom_rows(rows, filename, mode="replace")

    item_rows = _extract_item_list_rows(payload)
    if item_rows is not None:
        return import_uom_rows(item_rows, filename, mode="merge")

    raise WorkbookShapeError(
        "The uploaded file must be either a UOM workbook or an item list export."
    )


def import_uom_rows(rows: list[list[Any]], filename: str, mode: str = "replace") -> UomImport:
    import_log = UomImport(filename=filename, product_count=0)
    db.session.add(import_log)
    db.session.flush()

    existing_products = {
        product.sku_name: product for product in db.session.scalars(select(Product))
    }
    existing_by_normalized: dict[str, list[Product]] = {}
    for product in existing_products.values():
        existing_by_normalized.setdefault(product.normalized_name, []).append(product)
        if mode == "replace" and product.source_import_id is not None:
            product.is_active = False

    imported = 0
    skipped = 0
    deactivated = 0
    for row in rows:
        sku_name = _string_value(row[0])
        if not sku_name:
            continue

        product = existing_products.get(sku_name)
        normalized_name = normalize_sku(sku_name)
        is_active_row = True if len(row) < 7 else bool(row[6])
        if product is None and mode == "merge":
            normalized_matches = existing_by_normalized.get(normalized_name, [])
            if len(normalized_matches) == 1:
                matched_product = normalized_matches[0]
                if is_active_row:
                    skipped += 1
                else:
                    matched_product.is_active = False
                    matched_product.source_import_id = import_log.id
                    deactivated += 1
                continue

        if product is None:
            product = Product(sku_name=sku_name, normalized_name=normalized_name)
            db.session.add(product)
            existing_products[sku_name] = product
            existing_by_normalized.setdefault(normalized_name, []).append(product)
        elif mode == "merge":
            if is_active_row:
                skipped += 1
                continue
            product.is_active = False
            product.source_import_id = import_log.id
            deactivated += 1
            continue

        product.normalized_name = normalized_name
        product.uom = _string_value(row[1]) or None
        product.alt_uom = _string_value(row[2]) or None
        product.conversion = _decimal_value(row[3])
        product.vatable = _string_value(row[4]).lower() == "yes"
        product.price = _decimal_value(row[5])
        product.is_active = is_active_row
        product.source_import_id = import_log.id
        if is_active_row:
            imported += 1
        else:
            deactivated += 1

    import_log.product_count = imported
    import_log.skipped_count = skipped
    import_log.import_mode = mode
    import_log.deactivated_count = deactivated
    db.session.commit()
    return import_log


def bootstrap_seed_uom_if_empty() -> UomImport | None:
    if not SEED_UOM_PATH.exists():
        return None

    if db.session.scalar(select(func.count(UomImport.id))) not in (None, 0):
        return None

    rows: list[list[Any]] = []
    with SEED_UOM_PATH.open("r", encoding="utf-8", newline="") as seed_file:
        reader = csv.reader(seed_file)
        next(reader, None)
        for row in reader:
            rows.append(row[:6])

    if not rows:
        return None

    return import_uom_rows(rows, "LT to DN system 1.xlsx (seed)")


def save_product_master_entry(form_data: dict[str, Any], product_id: int | None = None) -> Product:
    sku_name = _string_value(form_data.get("sku_name"))
    if not sku_name:
        raise ServiceError("Product name is required.")

    price = _decimal_value(form_data.get("price"))
    if price is None:
        raise ServiceError("Rate/price is required.")

    product = db.session.get(Product, product_id) if product_id is not None else None

    duplicate = db.session.scalar(select(Product).where(Product.sku_name == sku_name))
    if duplicate is not None and (product is None or duplicate.id != product.id):
        raise ServiceError(f"'{sku_name}' already exists in the product master.")

    if product is None:
        product = Product(
            sku_name=sku_name,
            normalized_name=normalize_sku(sku_name),
            is_active=True,
            source_import_id=None,
        )
        db.session.add(product)

    product.sku_name = sku_name
    product.normalized_name = normalize_sku(sku_name)
    product.uom = _string_value(form_data.get("uom")) or None
    product.alt_uom = _string_value(form_data.get("alt_uom")) or None
    product.conversion = _decimal_value(form_data.get("conversion"))
    product.price = price
    product.vatable = _string_value(form_data.get("vatable")).lower() in {"yes", "true", "1", "on"}
    product.is_active = True
    if product.source_import_id is None:
        product.source_import_id = None

    db.session.commit()
    return product


def set_product_active(product_id: int, is_active: bool) -> Product:
    product = db.session.get(Product, product_id)
    if product is None:
        raise ServiceError("That product could not be found.")

    product.is_active = is_active
    db.session.commit()
    return product


def create_tracker_run(file_storage: Any, timezone_name: str) -> UploadRun:
    workbook = _load_workbook_from_upload(file_storage)
    sheet = _resolve_tracker_sheet(workbook)
    product_headers = []
    for column_index in range(3, sheet.max_column + 1):
        value = _string_value(sheet.cell(1, column_index).value)
        if value:
            product_headers.append((column_index, value))

    run = UploadRun(
        id=uuid4().hex,
        original_filename=file_storage.filename or "tracker.xlsx",
        invoice_date=tomorrow_in_timezone(timezone_name).strftime(DATE_FORMAT),
        status="needs_review",
        rows_detected=0,
        rows_ready=0,
        rows_needing_review=0,
    )
    db.session.add(run)
    db.session.flush()

    products = list(db.session.scalars(select(Product)))
    aliases = list(db.session.scalars(select(ProductAlias)))

    for row_index in range(2, sheet.max_row + 1):
        order_number = _string_value(sheet.cell(row_index, 1).value)
        supermarket = _string_value(sheet.cell(row_index, 2).value)
        if not order_number and not supermarket:
            continue

        for column_index, sku_name in product_headers:
            quantity = _decimal_value(sheet.cell(row_index, column_index).value)
            if quantity is None or quantity <= Decimal("0"):
                continue

            run.rows_detected += 1
            line = UploadLine(
                run_id=run.id,
                order_number=order_number,
                supermarket_name=supermarket,
                source_sku=sku_name,
                normalized_source_sku=normalize_sku(sku_name),
                quantity=quantity,
            )

            match = resolve_product_match(sku_name, products, aliases)
            if match is None:
                line.status = "needs_review"
                run.rows_needing_review += 1
            elif not match.product.is_active:
                line.status = "ignored"
                line.matched_by = "inactive"
                line.product_id = match.product.id
                line.resolved_sku_name = match.product.sku_name
            elif match.product.price is None:
                line.status = "needs_review"
                run.rows_needing_review += 1
            else:
                apply_product_to_line(line, match.product, match.match_method)
                run.rows_ready += 1

            db.session.add(line)

    run.status = "ready" if run.rows_needing_review == 0 else "needs_review"
    db.session.commit()
    return run


def build_run_summary(run_id: str) -> RunSummary | None:
    run = db.session.get(UploadRun, run_id)
    if run is None:
        return None

    unresolved_lines = list(
        db.session.scalars(
            select(UploadLine).where(UploadLine.run_id == run_id, UploadLine.status == "needs_review")
        )
    )

    grouped: dict[str, list[UploadLine]] = {}
    for line in unresolved_lines:
        grouped.setdefault(line.source_sku, []).append(line)

    products = list(db.session.scalars(select(Product).where(Product.is_active.is_(True))))
    groups = []
    for source_sku, lines in grouped.items():
        groups.append(
            UnresolvedGroup(
                source_sku=source_sku,
                occurrences=len(lines),
                order_numbers=sorted({line.order_number for line in lines}),
                supermarkets=sorted({line.supermarket_name for line in lines}),
                suggestions=suggest_products(source_sku, products),
            )
        )

    groups.sort(key=lambda item: item.source_sku)
    ignored_line_items = list(
        db.session.scalars(
            select(UploadLine).where(UploadLine.run_id == run_id, UploadLine.status == "ignored")
        )
    )
    ignored_grouped: dict[str, list[UploadLine]] = {}
    for line in ignored_line_items:
        ignored_grouped.setdefault(line.source_sku, []).append(line)

    ignored_groups = []
    for source_sku, lines in ignored_grouped.items():
        ignored_groups.append(
            IgnoredGroup(
                source_sku=source_sku,
                occurrences=len(lines),
                order_numbers=sorted({line.order_number for line in lines}),
                supermarkets=sorted({line.supermarket_name for line in lines}),
                reason="Inactive product",
            )
        )

    ignored_groups.sort(key=lambda item: item.source_sku)
    ready_lines = db.session.scalar(
        select(func.count(UploadLine.id)).where(UploadLine.run_id == run_id, UploadLine.status == "ready")
    ) or 0
    ignored_lines = db.session.scalar(
        select(func.count(UploadLine.id)).where(UploadLine.run_id == run_id, UploadLine.status == "ignored")
    ) or 0
    return RunSummary(
        run=run,
        unresolved_groups=groups,
        ignored_groups=ignored_groups,
        ready_lines=ready_lines,
        ignored_lines=ignored_lines,
    )


def apply_review_decisions(run_id: str, mapping: dict[str, int]) -> UploadRun:
    run = db.session.get(UploadRun, run_id)
    if run is None:
        raise WorkbookShapeError("This upload run could not be found.")

    for source_sku, product_id in mapping.items():
        product = db.session.get(Product, product_id)
        if product is None:
            raise WorkbookShapeError(f"Selected product for '{source_sku}' could not be found.")
        if product.price is None:
            raise WorkbookShapeError(f"'{product.sku_name}' still has no price in the UOM master.")

        alias = db.session.scalar(select(ProductAlias).where(ProductAlias.alias_name == source_sku))
        if alias is None:
            alias = ProductAlias(
                alias_name=source_sku,
                normalized_name=normalize_sku(source_sku),
                product_id=product.id,
                match_method="approved-alias",
            )
            db.session.add(alias)
        else:
            alias.product_id = product.id
            alias.normalized_name = normalize_sku(source_sku)
            alias.match_method = "approved-alias"

        lines = list(
            db.session.scalars(
                select(UploadLine).where(UploadLine.run_id == run_id, UploadLine.source_sku == source_sku)
            )
        )
        for line in lines:
            apply_product_to_line(line, product, "approved-alias")

    _refresh_run_totals(run)
    db.session.commit()
    return run


def mark_source_sku_inactive(run_id: str, source_sku: str) -> tuple[UploadRun, Product]:
    run = db.session.get(UploadRun, run_id)
    if run is None:
        raise WorkbookShapeError("This upload run could not be found.")

    sku_name = _string_value(source_sku)
    if not sku_name:
        raise WorkbookShapeError("The source SKU to mark inactive was empty.")

    product = db.session.scalar(select(Product).where(Product.sku_name == sku_name))
    if product is None:
        product = Product(
            sku_name=sku_name,
            normalized_name=normalize_sku(sku_name),
            is_active=False,
            source_import_id=None,
        )
        db.session.add(product)
        db.session.flush()
    else:
        product.is_active = False

    lines = list(
        db.session.scalars(
            select(UploadLine).where(UploadLine.run_id == run_id, UploadLine.source_sku == sku_name)
        )
    )
    if not lines:
        raise WorkbookShapeError(f"No review lines were found for '{sku_name}'.")

    for line in lines:
        line.status = "ignored"
        line.matched_by = "inactive"
        line.product_id = product.id
        line.resolved_sku_name = product.sku_name
        line.resolved_rate = None
        line.resolved_vatable = False

    _refresh_run_totals(run)
    db.session.commit()
    return run, product


def export_run_to_xls(run_id: str) -> tuple[str, bytes]:
    run = db.session.get(UploadRun, run_id)
    if run is None:
        raise WorkbookShapeError("This upload run could not be found.")
    if run.rows_needing_review > 0:
        raise WorkbookShapeError("Resolve all review items before downloading the final file.")

    lines = list(
        db.session.scalars(
            select(UploadLine)
            .where(UploadLine.run_id == run_id, UploadLine.status == "ready")
            .order_by(UploadLine.id.asc())
        )
    )
    workbook = xlwt.Workbook()
    sheet = workbook.add_sheet(TEMPLATE_SHEET)

    header_style = xlwt.easyxf(
        "font: bold on, height 240;"
        "pattern: pattern solid, fore_colour ocean_blue;"
        "align: horiz center;"
        "borders: left thin, right thin, top thin, bottom thin;"
    )
    text_style = xlwt.easyxf("borders: left thin, right thin, top thin, bottom thin;")
    date_style = xlwt.easyxf(
        "borders: left thin, right thin, top thin, bottom thin;",
        num_format_str="yyyy-mm-dd",
    )
    decimal_style = xlwt.easyxf(
        "borders: left thin, right thin, top thin, bottom thin;",
        num_format_str="0.00##",
    )

    for col_index, header in enumerate(OUTPUT_HEADERS):
        sheet.write(0, col_index, header, header_style)
        sheet.col(col_index).width = min(max(len(header) + 3, 14) * 256, 35 * 256)

    for row_index, line in enumerate(lines, start=1):
        amount = line.quantity * line.resolved_rate
        vat_amount = (amount * VAT_RATE / Decimal("100")).quantize(Decimal("0.01")) if line.resolved_vatable else ""
        row = [
            run.invoice_date,
            line.order_number,
            VOUCHER_TYPE_NAME,
            line.supermarket_name,
            line.resolved_sku_name,
            float(line.quantity),
            float(line.resolved_rate),
            float(amount),
            SALES_LEDGER_NAME,
            VAT_LABEL if line.resolved_vatable else "",
            float(VAT_RATE) if line.resolved_vatable else "",
            float(vat_amount) if vat_amount != "" else "",
        ]

        for col_index, value in enumerate(row):
            if col_index == 0:
                parsed = datetime.strptime(str(value), DATE_FORMAT)
                sheet.write(row_index, col_index, parsed, date_style)
            elif isinstance(value, (int, float)):
                sheet.write(row_index, col_index, value, decimal_style)
            else:
                sheet.write(row_index, col_index, value, text_style)

    stream = BytesIO()
    workbook.save(stream)
    stream.seek(0)
    run.status = "exported"
    run.exported_at = datetime.now(ZoneInfo(current_app.config["APP_TIMEZONE"]))
    db.session.commit()
    return _build_export_filename(run), stream.getvalue()


def resolve_product_match(source_sku: str, products: list[Product], aliases: list[ProductAlias]) -> ProductMatch | None:
    exact_lookup = {product.sku_name: product for product in products}
    if source_sku in exact_lookup:
        return ProductMatch(exact_lookup[source_sku], "exact")

    alias_lookup = {alias.alias_name: alias for alias in aliases}
    alias = alias_lookup.get(source_sku)
    if alias is not None:
        product = db.session.get(Product, alias.product_id)
        if product is not None:
            return ProductMatch(product, "approved-alias")

    normalized = normalize_sku(source_sku)
    normalized_products = [product for product in products if product.normalized_name == normalized]
    if len(normalized_products) == 1:
        return ProductMatch(normalized_products[0], "normalized")

    normalized_aliases = [alias for alias in aliases if alias.normalized_name == normalized]
    if len(normalized_aliases) == 1:
        product = db.session.get(Product, normalized_aliases[0].product_id)
        if product is not None:
            return ProductMatch(product, "approved-alias")

    return None


def suggest_products(source_sku: str, products: list[Product], limit: int = 5) -> list[Product]:
    normalized = normalize_sku(source_sku)
    ranked = []
    for product in products:
        score = SequenceMatcher(None, normalized, product.normalized_name).ratio()
        if score >= 0.42:
            ranked.append((score, product))

    ranked.sort(key=lambda item: (-item[0], item[1].sku_name))
    suggestions = []
    seen = set()
    for _, product in ranked:
        if product.id in seen:
            continue
        suggestions.append(product)
        seen.add(product.id)
        if len(suggestions) == limit:
            break
    return suggestions


def normalize_sku(value: str) -> str:
    cleaned = []
    for character in value.upper():
        cleaned.append(character if character.isalnum() else " ")
    return " ".join("".join(cleaned).split())


def tomorrow_in_timezone(timezone_name: str) -> datetime:
    return datetime.now(ZoneInfo(timezone_name)) + timedelta(days=1)


def _build_export_filename(run: UploadRun) -> str:
    source_label = _clean_filename_part(Path(run.original_filename or "tracker.xlsx").stem, "Tracker")
    invoice_label = _clean_filename_part(run.invoice_date, "Invoice Date")
    return f"DALA Delivery Note - {invoice_label} - {source_label}.xls"


def _clean_filename_part(value: str, fallback: str) -> str:
    cleaned = re.sub(INVALID_FILENAME_CHARS, " ", str(value))
    cleaned = cleaned.replace("_", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .-")
    if not cleaned:
        return fallback
    return cleaned[:90].rstrip(" .-")


def apply_product_to_line(line: UploadLine, product: Product, match_method: str) -> None:
    line.product_id = product.id
    line.status = "ready"
    line.matched_by = match_method
    line.resolved_sku_name = product.sku_name
    line.resolved_rate = product.price
    line.resolved_vatable = bool(product.vatable)


def _refresh_run_totals(run: UploadRun) -> None:
    run.rows_ready = db.session.scalar(
        select(func.count(UploadLine.id)).where(UploadLine.run_id == run.id, UploadLine.status == "ready")
    ) or 0
    run.rows_needing_review = db.session.scalar(
        select(func.count(UploadLine.id)).where(UploadLine.run_id == run.id, UploadLine.status == "needs_review")
    ) or 0
    run.status = "ready" if run.rows_needing_review == 0 else "needs_review"


def _load_workbook_from_upload(file_storage: Any):
    payload = _read_upload_payload(file_storage)
    workbook = _try_load_workbook(payload)
    if workbook is not None:
        return workbook
    raise WorkbookShapeError("The uploaded file could not be read as an Excel workbook.")


def _read_upload_payload(file_storage: Any) -> bytes:
    payload = file_storage.read()
    file_storage.stream.seek(0)
    return payload


def _try_load_workbook(payload: bytes):
    try:
        return load_workbook(BytesIO(payload), data_only=True)
    except Exception:  # pragma: no cover
        return None


def _extract_item_list_rows(payload: bytes) -> list[list[Any]] | None:
    text = _decode_text_payload(payload)
    if text is None:
        return None

    reader = csv.DictReader(StringIO(text), delimiter="\t")
    if reader.fieldnames is None:
        return None

    normalized_headers = {(_normalize_header(name) if name is not None else "") for name in reader.fieldnames}
    required_headers = {"item name", "cases size", "item ptr"}
    if not required_headers.issubset(normalized_headers):
        return None

    rows: list[list[Any]] = []
    for row in reader:
        cleaned = {_normalize_header(key): value for key, value in row.items() if key is not None}
        sku_name = _string_value(cleaned.get("item name"))
        if not sku_name:
            continue

        status = _string_value(cleaned.get("status")).lower()
        is_active_row = status != "inactive" and status != "deactivated"

        price = _decimal_value(cleaned.get("item ptr"))
        if price is None and is_active_row:
            continue

        tax_rate = _decimal_value(cleaned.get("tax rate"))
        rows.append(
            [
                sku_name,
                "ctn",
                "unt",
                _decimal_value(cleaned.get("cases size")),
                "Yes" if tax_rate is not None and tax_rate > 0 else "No",
                price,
                is_active_row,
            ]
        )

    return rows or None


def _decode_text_payload(payload: bytes) -> str | None:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return None


def _normalize_header(value: Any) -> str:
    return " ".join(_string_value(value).lower().split())


def _string_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _decimal_value(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _resolve_tracker_sheet(workbook: Any):
    best_sheet = None
    best_score = 0

    for sheet_name in workbook.sheetnames:
        sheet = workbook[sheet_name]
        score = _tracker_sheet_score(sheet)
        if _normalize_header(sheet_name) == TRACKER_SHEET:
            score += 2
        if score > best_score:
            best_score = score
            best_sheet = sheet

    if best_sheet is not None and best_score >= 8:
        return best_sheet

    raise WorkbookShapeError(
        "We could not find the sheet that contains order numbers, store names, and product quantities."
    )


def _looks_like_tracker_sheet(sheet: Any) -> bool:
    return _tracker_sheet_score(sheet) >= 8


def _tracker_sheet_score(sheet: Any) -> int:
    if sheet.max_row < 2 or sheet.max_column < 3:
        return 0

    first_header = _normalize_header(sheet.cell(1, 1).value)
    second_header = _normalize_header(sheet.cell(1, 2).value)
    product_headers = sum(1 for column_index in range(3, sheet.max_column + 1) if _string_value(sheet.cell(1, column_index).value))
    sample_rows = min(sheet.max_row, 8)
    row_identity_hits = 0
    quantity_hits = 0

    for row_index in range(2, sample_rows + 1):
        if _string_value(sheet.cell(row_index, 1).value) and _string_value(sheet.cell(row_index, 2).value):
            row_identity_hits += 1

        for column_index in range(3, min(sheet.max_column, 20) + 1):
            value = _decimal_value(sheet.cell(row_index, column_index).value)
            if value is not None:
                quantity_hits += 1
                break

    score = 0
    if sheet.max_column >= TRACKER_MIN_COLUMNS:
        score += 4
    if first_header in TRACKER_ORDER_HEADERS:
        score += 4
    if second_header in TRACKER_STORE_HEADERS:
        score += 4
    if product_headers >= 5:
        score += 2
    if row_identity_hits >= 1:
        score += 2
    if quantity_hits >= 1:
        score += 4
    return score


def _normalize_header(value: Any) -> str:
    return " ".join(_string_value(value).lower().split())
