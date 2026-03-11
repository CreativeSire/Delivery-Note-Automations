from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from io import BytesIO
from pathlib import Path
from typing import Any
from uuid import uuid4

from openpyxl import load_workbook
from sqlalchemy import func, select

from models import (
    LoadingTrackerDay,
    LoadingTrackerFeeItem,
    LoadingTrackerImport,
    LoadingTrackerInventoryItem,
    LoadingTrackerRow,
    LoadingTrackerRowItem,
    db,
)

DAY_SHEET_NAMES = ["Mon", "Tues", "Wed", "Thurs", "Fri", "Sat"]
LL_SHEET_MAP = {
    "Mon": "LL Mon",
    "Tues": "LL Tue",
    "Wed": "LL Wed",
    "Thurs": "LLThurs",
    "Fri": "LL Fri",
    "Sat": "LL Sat",
}
PLANNING_SUMMARY_LABELS = {
    "g2g_total": "Expected in G2G For Loading",
    "loaded_total": "TOTAL LOADED OUT FOR DELIVERY",
    "remaining_total": "Remaining Inventory After Loading",
    "expected_store_total": "Expected in Store For Loading",
}
PLANNED_STATE = "planned"
PENDING_STATE = "pending"
PENDING_SENTINEL = "__pending__"
INVALID_FILENAME_CHARS = r'[<>:"/\\|?*\x00-\x1f]'
DEFAULT_BATCHES = ["Load 1", "Load 2", "Load 3", "Load 4", "Unassigned"]


class LoadingTrackerError(Exception):
    pass


@dataclass
class LoadingTrackerSummary:
    import_count: int
    latest_import: LoadingTrackerImport | None
    recent_imports: list[LoadingTrackerImport]
    total_batches: int
    total_active_stores: int
    total_pending_stores: int
    total_fee_rows: int


def build_loading_tracker_summary() -> LoadingTrackerSummary:
    latest_import = db.session.scalar(
        select(LoadingTrackerImport).order_by(LoadingTrackerImport.created_at.desc()).limit(1)
    )
    total_batches = 0
    total_active_stores = 0
    total_pending_stores = 0
    total_fee_rows = 0

    if latest_import is not None:
        if latest_import.planning_rows:
            total_batches = sum(
                len({row.batch_name for row in day.planning_rows if row.row_state == PLANNED_STATE})
                for day in latest_import.days
            )
            total_active_stores = sum(
                len([row for row in day.planning_rows if row.row_state == PLANNED_STATE])
                for day in latest_import.days
            )
            total_pending_stores = len([row for row in latest_import.planning_rows if row.row_state == PENDING_STATE])
            total_fee_rows = len(latest_import.fee_items)
        else:
            total_batches = sum(day.batch_count for day in latest_import.days)
            total_active_stores = sum(day.active_store_count for day in latest_import.days)
            total_pending_stores = len(latest_import.pending_rows_json or [])
            total_fee_rows = latest_import.fees_row_count or 0

    return LoadingTrackerSummary(
        import_count=db.session.scalar(select(func.count(LoadingTrackerImport.id))) or 0,
        latest_import=latest_import,
        recent_imports=list(
            db.session.scalars(select(LoadingTrackerImport).order_by(LoadingTrackerImport.created_at.desc()).limit(5))
        ),
        total_batches=total_batches,
        total_active_stores=total_active_stores,
        total_pending_stores=total_pending_stores,
        total_fee_rows=total_fee_rows,
    )


def import_loading_tracker_workbook(file_storage: Any) -> LoadingTrackerImport:
    workbook = _load_workbook_from_upload(file_storage)
    filename = file_storage.filename or "loading-tracker.xlsx"

    present_day_sheets = [name for name in DAY_SHEET_NAMES if name in workbook.sheetnames]
    if not present_day_sheets:
        raise LoadingTrackerError("The workbook must include at least one day sheet like Mon or Tues.")

    tracker_import = LoadingTrackerImport(
        id=uuid4().hex,
        filename=filename,
        week_label=_clean_filename_part(Path(filename).stem, "Loading Tracker"),
    )
    db.session.add(tracker_import)
    db.session.flush()

    assumptions_sheet = workbook["Assumptions"] if "Assumptions" in workbook.sheetnames else None
    if assumptions_sheet is not None:
        assumptions = _parse_assumptions_sheet(assumptions_sheet)
        tracker_import.assumptions_sku_count = assumptions["sku_count"]
        tracker_import.assumptions_store_count = assumptions["store_count"]

    opening_sheet = workbook["Opening Inventory"] if "Opening Inventory" in workbook.sheetnames else None
    if opening_sheet is not None:
        opening = _parse_inventory_sheet(opening_sheet)
        tracker_import.opening_g2g_total = opening["g2g_total"]
        tracker_import.opening_remaining_total = opening["remaining_total"]
        tracker_import.opening_top_products_json = opening["top_products"]
        for sort_order, item in enumerate(opening["inventory_items"], start=1):
            db.session.add(
                LoadingTrackerInventoryItem(
                    tracker_import_id=tracker_import.id,
                    sku_name=item["sku"],
                    opening_g2g_qty=_decimal_or_none(item["opening_g2g_qty"]),
                    opening_remaining_qty=_decimal_or_none(item["opening_remaining_qty"]),
                    added_qty=Decimal("0"),
                    sort_order=sort_order,
                )
            )

    pending_sheet = workbook["Pending Orders"] if "Pending Orders" in workbook.sheetnames else None
    if pending_sheet is not None:
        pending = _parse_support_sheet(pending_sheet)
        tracker_import.pending_g2g_total = pending["g2g_total"]
        tracker_import.pending_loaded_total = pending["loaded_total"]
        tracker_import.pending_remaining_total = pending["remaining_total"]
        tracker_import.pending_rows_json = pending["store_rows"]
        tracker_import.pending_top_products_json = pending["top_products"]
        for sort_order, row_data in enumerate(pending["store_rows"], start=1):
            _save_row_record(
                tracker_import=tracker_import,
                row_data=row_data,
                row_state=PENDING_STATE,
                day=None,
                sort_order=sort_order,
                source_kind="import",
                reason_text=row_data.get("reason_text") or "Imported from Pending Orders",
            )

    fee_sheet = _resolve_fee_sheet(workbook)
    if fee_sheet is not None:
        fees = _parse_fee_sheet(fee_sheet)
        tracker_import.fees_row_count = fees["row_count"]
        tracker_import.fees_total_delivery_value = fees["total_delivery_value"]
        tracker_import.fees_total_payment_value = fees["total_payment_value"]
        tracker_import.fee_rows_json = fees["top_rows"]
        for row in fees["rows"]:
            db.session.add(
                LoadingTrackerFeeItem(
                    tracker_import_id=tracker_import.id,
                    brand_partner=row["brand_partner"] or None,
                    sku_name=row["sku"],
                    vatable_text=row["vatable"] or None,
                    retail_delivery_value=_decimal_or_none(row["retail_delivery_value"]),
                    payment_collection_value=_decimal_or_none(row["payment_collection_value"]),
                )
            )

    notes_sheet = workbook["NOTES FOR USER"] if "NOTES FOR USER" in workbook.sheetnames else None
    if notes_sheet is not None:
        tracker_import.notes_count = _count_note_lines(notes_sheet)

    for order, day_name in enumerate(present_day_sheets, start=1):
        planning_sheet = workbook[day_name]
        load_sheet = workbook[LL_SHEET_MAP[day_name]] if LL_SHEET_MAP.get(day_name) in workbook.sheetnames else None
        parsed_day = _parse_day_sheet(planning_sheet, day_name, load_sheet)
        day_record = LoadingTrackerDay(
            tracker_import_id=tracker_import.id,
            day_name=day_name,
            day_order=order,
            g2g_total=parsed_day["g2g_total"],
            loaded_total=parsed_day["loaded_total"],
            remaining_total=parsed_day["remaining_total"],
            expected_store_total=parsed_day["expected_store_total"],
            batch_count=parsed_day["batch_count"],
            active_store_count=parsed_day["active_store_count"],
            total_weight=parsed_day["total_weight"],
            total_value=parsed_day["total_value"],
            load_1_total=parsed_day["load_1_total"],
            load_2_total=parsed_day["load_2_total"],
            load_3_total=parsed_day["load_3_total"],
            load_4_total=parsed_day["load_4_total"],
            load_total=parsed_day["load_total"],
            store_rows_json=parsed_day["store_rows"],
            top_products_json=parsed_day["top_products"],
            load_rows_json=parsed_day["load_rows"],
        )
        db.session.add(day_record)
        db.session.flush()

        for sort_order, row_data in enumerate(parsed_day["store_rows"], start=1):
            _save_row_record(
                tracker_import=tracker_import,
                row_data=row_data,
                row_state=PLANNED_STATE,
                day=day_record,
                sort_order=sort_order,
                source_kind="import",
            )

    db.session.commit()
    return tracker_import


def get_loading_tracker_import(import_id: str | None = None) -> LoadingTrackerImport | None:
    if import_id:
        return db.session.get(LoadingTrackerImport, import_id)
    return db.session.scalar(select(LoadingTrackerImport).order_by(LoadingTrackerImport.created_at.desc()).limit(1))


def get_loading_tracker_day(import_id: str, day_name: str) -> LoadingTrackerDay | None:
    return db.session.scalar(
        select(LoadingTrackerDay)
        .where(LoadingTrackerDay.tracker_import_id == import_id, LoadingTrackerDay.day_name == day_name)
        .limit(1)
    )


def get_loading_tracker_row(row_id: int) -> LoadingTrackerRow | None:
    return db.session.get(LoadingTrackerRow, row_id)


def build_loading_tracker_overview(tracker_import: LoadingTrackerImport | None) -> dict[str, Any]:
    if tracker_import is None:
        return {
            "day_count": 0,
            "total_batches": 0,
            "total_active_stores": 0,
            "total_pending_rows": 0,
            "total_load_value": Decimal("0"),
            "largest_day_name": None,
            "largest_day_value": Decimal("0"),
            "live_ready": False,
        }

    if tracker_import.planning_rows:
        day_contexts = [build_loading_tracker_day_context(day) for day in tracker_import.days]
        largest_day = max(day_contexts, key=lambda item: item["metrics"]["total_value"], default=None)
        return {
            "day_count": len(tracker_import.days),
            "total_batches": sum(item["metrics"]["batch_count"] for item in day_contexts),
            "total_active_stores": sum(item["metrics"]["active_store_count"] for item in day_contexts),
            "total_pending_rows": len([row for row in tracker_import.planning_rows if row.row_state == PENDING_STATE]),
            "total_load_value": sum((item["metrics"]["total_value_decimal"] for item in day_contexts), Decimal("0")),
            "largest_day_name": largest_day["day"].day_name if largest_day else None,
            "largest_day_value": largest_day["metrics"]["total_value_decimal"] if largest_day else Decimal("0"),
            "live_ready": True,
        }

    days = tracker_import.days or []
    largest_day = max(days, key=lambda day: day.total_value or 0, default=None)
    return {
        "day_count": len(days),
        "total_batches": sum(day.batch_count for day in days),
        "total_active_stores": sum(day.active_store_count for day in days),
        "total_pending_rows": len(tracker_import.pending_rows_json or []),
        "total_load_value": sum((day.total_value or Decimal("0")) for day in days),
        "largest_day_name": largest_day.day_name if largest_day else None,
        "largest_day_value": largest_day.total_value or Decimal("0") if largest_day else Decimal("0"),
        "live_ready": False,
    }


def build_loading_tracker_day_context(day: LoadingTrackerDay) -> dict[str, Any]:
    tracker_import = day.tracker_import
    planned_rows = [row for row in day.planning_rows if row.row_state == PLANNED_STATE]
    serialized_rows = [_serialize_row(row) for row in planned_rows]
    grouped_batches = group_store_rows_by_batch(serialized_rows)

    inventory_totals = _inventory_totals_by_sku(tracker_import)
    previous_days = [item for item in tracker_import.days if item.day_order < day.day_order]
    consumed_before = _aggregate_row_item_totals(_rows_for_days(previous_days))
    consumed_today = _aggregate_row_item_totals(planned_rows)

    available_start = _subtract_maps(inventory_totals, consumed_before)
    remaining_after = _subtract_maps(available_start, consumed_today)
    ll_rows, load_totals = _build_ll_rows(serialized_rows)
    top_products = _sorted_top_products(consumed_today, 12)
    inventory_warnings = [
        {"sku": sku, "remaining": round(quantity, 2)}
        for sku, quantity in sorted(remaining_after.items(), key=lambda item: item[1])
        if quantity <= 0
    ][:8]

    total_weight = round(sum(row["weight"] for row in serialized_rows), 2)
    total_value = round(sum(row["value"] for row in serialized_rows), 2)
    total_quantity = round(sum(row["total_quantity"] for row in serialized_rows), 2)

    return {
        "day": day,
        "metrics": {
            "g2g_total": round(sum(available_start.values()), 2),
            "loaded_total": total_quantity,
            "remaining_total": round(sum(remaining_after.values()), 2),
            "expected_store_total": total_quantity,
            "batch_count": len(grouped_batches),
            "active_store_count": len(serialized_rows),
            "total_weight": total_weight,
            "total_value": total_value,
            "total_value_decimal": Decimal(f"{total_value:.4f}"),
            "load_1_total": round(load_totals.get("load_1", 0.0), 2),
            "load_2_total": round(load_totals.get("load_2", 0.0), 2),
            "load_3_total": round(load_totals.get("load_3", 0.0), 2),
            "load_4_total": round(load_totals.get("load_4", 0.0), 2),
            "load_total": round(load_totals.get("total", 0.0), 2),
        },
        "rows": serialized_rows,
        "grouped_batches": grouped_batches,
        "ll_rows": ll_rows,
        "top_products": top_products,
        "inventory_warnings": inventory_warnings,
        "day_options": [{"value": item.day_name, "label": item.day_name} for item in tracker_import.days],
    }


def build_loading_tracker_pending_context(tracker_import: LoadingTrackerImport) -> dict[str, Any]:
    pending_rows = [row for row in tracker_import.planning_rows if row.row_state == PENDING_STATE]
    serialized_rows = [_serialize_row(row) for row in pending_rows]
    grouped_batches = group_store_rows_by_batch(serialized_rows)
    top_products = _sorted_top_products(_aggregate_row_item_totals(pending_rows), 12)
    return {
        "rows": serialized_rows,
        "grouped_batches": grouped_batches,
        "top_products": top_products,
        "total_rows": len(serialized_rows),
        "total_quantity": round(sum(row["total_quantity"] for row in serialized_rows), 2),
        "total_value": round(sum(row["value"] for row in serialized_rows), 2),
        "day_options": [{"value": item.day_name, "label": item.day_name} for item in tracker_import.days],
    }


def build_loading_tracker_inventory_context(tracker_import: LoadingTrackerImport) -> dict[str, Any]:
    inventory_rows: list[dict[str, Any]] = []
    inventory_totals = _inventory_totals_by_sku(tracker_import)
    planned_totals = _aggregate_row_item_totals(_rows_for_days(tracker_import.days))
    all_skus = sorted(set(inventory_totals) | set(planned_totals))
    inventory_lookup = {item.sku_name: item for item in tracker_import.inventory_items}

    for sku in all_skus:
        record = inventory_lookup.get(sku)
        opening = _float_value(record.opening_g2g_qty) if record else 0.0
        added = _float_value(record.added_qty) if record else 0.0
        planned = round(planned_totals.get(sku, 0.0), 2)
        remaining = round(opening + added - planned, 2)
        inventory_rows.append(
            {
                "id": record.id if record else None,
                "sku": sku,
                "opening_g2g_qty": round(opening, 2),
                "opening_remaining_qty": round(_float_value(record.opening_remaining_qty), 2) if record else 0.0,
                "added_qty": round(added, 2),
                "planned_qty": planned,
                "remaining_qty": remaining,
                "status": "tight" if remaining <= 0 else "healthy",
            }
        )

    inventory_rows.sort(key=lambda item: (item["remaining_qty"], item["sku"]))
    return {
        "rows": inventory_rows,
        "g2g_total": round(sum(row["opening_g2g_qty"] + row["added_qty"] for row in inventory_rows), 2),
        "planned_total": round(sum(row["planned_qty"] for row in inventory_rows), 2),
        "remaining_total": round(sum(row["remaining_qty"] for row in inventory_rows), 2),
        "tight_count": len([row for row in inventory_rows if row["remaining_qty"] <= 0]),
    }


def build_loading_tracker_fees_context(tracker_import: LoadingTrackerImport) -> dict[str, Any]:
    fee_rows = [
        {
            "brand_partner": item.brand_partner or "",
            "sku": item.sku_name,
            "vatable": item.vatable_text or "",
            "retail_delivery_value": round(_float_value(item.retail_delivery_value), 2),
            "payment_collection_value": round(_float_value(item.payment_collection_value), 2),
        }
        for item in tracker_import.fee_items
    ]
    fee_rows.sort(key=lambda row: (-row["retail_delivery_value"], row["sku"]))
    return {
        "rows": fee_rows,
        "row_count": len(fee_rows),
        "total_delivery_value": round(sum(row["retail_delivery_value"] for row in fee_rows), 2),
        "total_payment_value": round(sum(row["payment_collection_value"] for row in fee_rows), 2),
    }


def build_loading_tracker_row_editor(
    tracker_import: LoadingTrackerImport,
    row: LoadingTrackerRow | None = None,
    selected_day_name: str | None = None,
) -> dict[str, Any]:
    if row is not None:
        current_day_name = row.day.day_name if row.day is not None else PENDING_SENTINEL
        selected_day_name = selected_day_name or current_day_name
    else:
        selected_day_name = selected_day_name or tracker_import.days[0].day_name

    return {
        "row": row,
        "selected_day_name": selected_day_name,
        "day_options": [{"value": day.day_name, "label": day.day_name} for day in tracker_import.days]
        + [{"value": PENDING_SENTINEL, "label": "Pending"}],
        "batch_options": DEFAULT_BATCHES,
        "items_text": _row_items_text(row) if row is not None else "",
    }


def save_loading_tracker_row(
    tracker_import_id: str,
    form_data: dict[str, Any],
    row_id: int | None = None,
) -> LoadingTrackerRow:
    tracker_import = get_loading_tracker_import(tracker_import_id)
    if tracker_import is None:
        raise LoadingTrackerError("The selected loading tracker import could not be found.")

    row = get_loading_tracker_row(row_id) if row_id is not None else None
    if row_id is not None and (row is None or row.tracker_import_id != tracker_import_id):
        raise LoadingTrackerError("The planning row could not be found.")

    store_name = _string_value(form_data.get("store_name"))
    if not store_name:
        raise LoadingTrackerError("Store name is required.")

    items = _parse_items_text(_string_value(form_data.get("items_text")))
    if not items:
        raise LoadingTrackerError("Add at least one SKU quantity in the planner.")

    selected_day_name = _string_value(form_data.get("target_day_name")) or PENDING_SENTINEL
    day = None
    row_state = PENDING_STATE
    if selected_day_name != PENDING_SENTINEL:
        day = get_loading_tracker_day(tracker_import_id, selected_day_name)
        if day is None:
            raise LoadingTrackerError("The selected planning day could not be found.")
        row_state = PLANNED_STATE

    if row is None:
        row = LoadingTrackerRow(tracker_import_id=tracker_import_id, source_kind="manual")
        db.session.add(row)

    row.day_id = day.id if day is not None else None
    row.row_state = row_state
    row.batch_name = _string_value(form_data.get("batch_name")) or "Unassigned"
    row.store_name = store_name
    row.contact = _string_value(form_data.get("contact")) or None
    row.lp = _string_value(form_data.get("lp")) or None
    row.tier = _string_value(form_data.get("tier")) or None
    row.region = _string_value(form_data.get("region")) or None
    row.delivery_date = _string_value(form_data.get("delivery_date")) or None
    row.reason_text = _string_value(form_data.get("reason_text")) or None
    row.total_weight = _decimal_value(form_data.get("total_weight"))
    row.total_value = _decimal_value(form_data.get("total_value"))
    row.sort_order = _next_sort_order(tracker_import, day, row_state, row.id)

    row.items.clear()
    for sku_name, quantity in items:
        row.items.append(
            LoadingTrackerRowItem(
                sku_name=sku_name,
                quantity=_decimal_or_none(quantity) or Decimal("0"),
            )
        )

    db.session.commit()
    return row


def move_loading_tracker_row(
    tracker_import_id: str,
    row_id: int,
    target_day_name: str,
    reason_text: str | None = None,
) -> LoadingTrackerRow:
    row = get_loading_tracker_row(row_id)
    if row is None or row.tracker_import_id != tracker_import_id:
        raise LoadingTrackerError("The planning row could not be found.")

    if target_day_name == PENDING_SENTINEL:
        row.day_id = None
        row.row_state = PENDING_STATE
        row.reason_text = reason_text or row.reason_text or "Held for later planning"
        row.sort_order = _next_sort_order(row.tracker_import, None, PENDING_STATE, row.id)
    else:
        day = get_loading_tracker_day(tracker_import_id, target_day_name)
        if day is None:
            raise LoadingTrackerError("The selected planning day could not be found.")
        row.day_id = day.id
        row.row_state = PLANNED_STATE
        row.reason_text = None
        row.sort_order = _next_sort_order(row.tracker_import, day, PLANNED_STATE, row.id)

    db.session.commit()
    return row


def save_inventory_adjustment(tracker_import_id: str, form_data: dict[str, Any]) -> LoadingTrackerInventoryItem:
    tracker_import = get_loading_tracker_import(tracker_import_id)
    if tracker_import is None:
        raise LoadingTrackerError("The selected loading tracker import could not be found.")

    sku_name = _string_value(form_data.get("sku_name"))
    if not sku_name:
        raise LoadingTrackerError("SKU name is required for the inventory adjustment.")

    item = next((entry for entry in tracker_import.inventory_items if entry.sku_name == sku_name), None)
    if item is None:
        item = LoadingTrackerInventoryItem(
            tracker_import_id=tracker_import_id,
            sku_name=sku_name,
            sort_order=(max((entry.sort_order for entry in tracker_import.inventory_items), default=0) + 1),
        )
        db.session.add(item)

    opening_g2g = _decimal_value(form_data.get("opening_g2g_qty"))
    opening_remaining = _decimal_value(form_data.get("opening_remaining_qty"))
    added_qty = _decimal_value(form_data.get("added_qty"))
    item.opening_g2g_qty = opening_g2g or Decimal("0")
    item.opening_remaining_qty = opening_remaining or Decimal("0")
    item.added_qty = added_qty or Decimal("0")

    db.session.commit()
    return item


def group_store_rows_by_batch(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows or []:
        grouped.setdefault(row.get("batch_name", "Unassigned"), []).append(row)

    ordered = []
    for batch_name in sorted(grouped, key=_batch_sort_key):
        batch_rows = grouped[batch_name]
        ordered.append(
            {
                "batch_name": batch_name,
                "store_count": len(batch_rows),
                "total_quantity": round(sum(row.get("total_quantity", 0) for row in batch_rows), 2),
                "total_weight": round(sum(row.get("weight", 0) for row in batch_rows), 2),
                "total_value": round(sum(row.get("value", 0) for row in batch_rows), 2),
                "rows": batch_rows,
            }
        )
    return ordered


def _save_row_record(
    tracker_import: LoadingTrackerImport,
    row_data: dict[str, Any],
    row_state: str,
    day: LoadingTrackerDay | None,
    sort_order: int,
    source_kind: str,
    reason_text: str | None = None,
) -> None:
    record = LoadingTrackerRow(
        tracker_import_id=tracker_import.id,
        day_id=day.id if day is not None else None,
        row_state=row_state,
        source_kind=source_kind,
        batch_name=row_data.get("batch_name") or "Unassigned",
        store_name=row_data.get("store_name") or "Unnamed store",
        contact=row_data.get("contact") or None,
        lp=row_data.get("lp") or None,
        tier=row_data.get("tier") or None,
        region=row_data.get("region") or None,
        delivery_date=row_data.get("delivery_date") or None,
        reason_text=reason_text,
        total_weight=_decimal_or_none(row_data.get("weight", 0.0)),
        total_value=_decimal_or_none(row_data.get("value", 0.0)),
        sort_order=sort_order,
    )
    db.session.add(record)
    db.session.flush()

    for item in row_data.get("items", []):
        db.session.add(
            LoadingTrackerRowItem(
                row_id=record.id,
                sku_name=item["sku"],
                quantity=_decimal_or_none(item["quantity"]) or Decimal("0"),
            )
        )


def _rows_for_days(days: list[LoadingTrackerDay]) -> list[LoadingTrackerRow]:
    rows: list[LoadingTrackerRow] = []
    for day in days:
        rows.extend([row for row in day.planning_rows if row.row_state == PLANNED_STATE])
    return rows


def _aggregate_row_item_totals(rows: list[LoadingTrackerRow]) -> dict[str, float]:
    totals: dict[str, float] = {}
    for row in rows:
        for item in row.items:
            quantity = _float_value(item.quantity)
            if quantity <= 0:
                continue
            totals[item.sku_name] = totals.get(item.sku_name, 0.0) + quantity
    return totals


def _inventory_totals_by_sku(tracker_import: LoadingTrackerImport) -> dict[str, float]:
    totals: dict[str, float] = {}
    for item in tracker_import.inventory_items:
        totals[item.sku_name] = round(_float_value(item.opening_g2g_qty) + _float_value(item.added_qty), 4)
    return totals


def _subtract_maps(left: dict[str, float], right: dict[str, float]) -> dict[str, float]:
    keys = set(left) | set(right)
    return {key: round(left.get(key, 0.0) - right.get(key, 0.0), 4) for key in keys}


def _serialize_row(row: LoadingTrackerRow) -> dict[str, Any]:
    items = [
        {"sku": item.sku_name, "quantity": round(_float_value(item.quantity), 2)}
        for item in row.items
        if _float_value(item.quantity) > 0
    ]
    items.sort(key=lambda item: (-item["quantity"], item["sku"]))
    return {
        "id": row.id,
        "row_state": row.row_state,
        "batch_name": row.batch_name or "Unassigned",
        "store_name": row.store_name,
        "contact": row.contact or "",
        "lp": row.lp or "",
        "tier": row.tier or "",
        "region": row.region or "",
        "delivery_date": row.delivery_date or "",
        "reason_text": row.reason_text or "",
        "weight": round(_float_value(row.total_weight), 2),
        "value": round(_float_value(row.total_value), 2),
        "total_quantity": round(sum(item["quantity"] for item in items), 2),
        "product_count": len(items),
        "items": items,
        "top_items": items[:5],
    }


def _build_ll_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, float]]:
    sku_totals: dict[str, dict[str, float]] = {}
    totals = {"load_1": 0.0, "load_2": 0.0, "load_3": 0.0, "load_4": 0.0, "total": 0.0}
    for row in rows:
        batch_key = _batch_to_key(row["batch_name"])
        for item in row["items"]:
            sku_totals.setdefault(
                item["sku"],
                {"load_1": 0.0, "load_2": 0.0, "load_3": 0.0, "load_4": 0.0, "total": 0.0},
            )
            sku_totals[item["sku"]][batch_key] += item["quantity"]
            sku_totals[item["sku"]]["total"] += item["quantity"]
            totals[batch_key] += item["quantity"]
            totals["total"] += item["quantity"]

    ll_rows = []
    for sku_name, item_totals in sorted(sku_totals.items(), key=lambda entry: (-entry[1]["total"], entry[0])):
        ll_rows.append(
            {
                "sku": sku_name,
                "load_1": round(item_totals["load_1"], 2),
                "load_2": round(item_totals["load_2"], 2),
                "load_3": round(item_totals["load_3"], 2),
                "load_4": round(item_totals["load_4"], 2),
                "total": round(item_totals["total"], 2),
            }
        )

    return ll_rows, totals


def _batch_to_key(batch_name: str) -> str:
    lowered = (batch_name or "").lower()
    if "2" in lowered:
        return "load_2"
    if "3" in lowered:
        return "load_3"
    if "4" in lowered:
        return "load_4"
    return "load_1"


def _row_items_text(row: LoadingTrackerRow | None) -> str:
    if row is None:
        return ""
    items = []
    for item in row.items:
        quantity = round(_float_value(item.quantity), 2)
        if quantity <= 0:
            continue
        items.append(f"{item.sku_name} = {quantity:g}")
    return "\n".join(items)


def _parse_items_text(text: str) -> list[tuple[str, float]]:
    parsed: list[tuple[str, float]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        separator = None
        for candidate in ("=", "\t", "|", ":"):
            if candidate in line:
                separator = candidate
                break
        if separator is not None:
            sku_name, quantity_text = line.rsplit(separator, 1)
        else:
            parts = line.rsplit(" ", 1)
            if len(parts) != 2:
                raise LoadingTrackerError(f"Could not understand planner line '{line}'. Use 'SKU = qty'.")
            sku_name, quantity_text = parts
        sku_name = sku_name.strip().strip("-")
        quantity = _float_value(quantity_text)
        if not sku_name or quantity <= 0:
            continue
        parsed.append((sku_name, round(quantity, 4)))
    return parsed


def _next_sort_order(
    tracker_import: LoadingTrackerImport,
    day: LoadingTrackerDay | None,
    row_state: str,
    current_row_id: int | None = None,
) -> int:
    rows = [
        row
        for row in tracker_import.planning_rows
        if row.id != current_row_id
        and row.row_state == row_state
        and ((row.day_id == day.id) if day is not None else row.day_id is None)
    ]
    return max((row.sort_order for row in rows), default=0) + 1


def _parse_day_sheet(sheet: Any, day_name: str, load_sheet: Any | None) -> dict[str, Any]:
    grid = _locate_day_grid(sheet)
    summary_rows = {
        key: _parse_metric_row(sheet, grid["product_headers"], label)
        for key, label in PLANNING_SUMMARY_LABELS.items()
    }

    batch_rows = []
    active_product_totals: dict[str, float] = {}
    current_batch = ""
    row_index = grid["header_row_index"] + 1

    while row_index <= sheet.max_row:
        batch_value = _string_value(sheet.cell(row_index, 1).value)
        if batch_value.lower().startswith("load "):
            current_batch = batch_value
            row_index += 1
            continue

        if _normalize_text(sheet.cell(row_index, grid["store_col"]).value) == "load for external delivery":
            row_index += 1
            continue

        store_name = _string_value(sheet.cell(row_index, grid["store_col"]).value)
        quantities: list[dict[str, Any]] = []
        total_quantity = 0.0
        for column_index, sku_name in grid["product_headers"]:
            quantity = _float_value(sheet.cell(row_index, column_index).value)
            if quantity <= 0:
                continue
            total_quantity += quantity
            quantities.append({"sku": sku_name, "quantity": round(quantity, 2)})
            active_product_totals[sku_name] = active_product_totals.get(sku_name, 0.0) + quantity

        weight = _float_value(sheet.cell(row_index, grid["weight_col"]).value) if grid["weight_col"] else 0.0
        value = _float_value(sheet.cell(row_index, grid["value_col"]).value) if grid["value_col"] else 0.0

        if not store_name and total_quantity == 0 and weight == 0 and value == 0:
            row_index += 1
            continue

        if not store_name or store_name.lower() in {"item", "load for external delivery"}:
            row_index += 1
            continue

        if total_quantity <= 0 and value <= 0 and weight <= 0:
            row_index += 1
            continue

        quantities.sort(key=lambda item: (-item["quantity"], item["sku"]))
        batch_rows.append(
            {
                "batch_name": current_batch or "Unassigned",
                "store_name": store_name,
                "contact": _string_value(sheet.cell(row_index, grid["contact_col"]).value) if grid["contact_col"] else "",
                "lp": _string_value(sheet.cell(row_index, grid["lp_col"]).value) if grid["lp_col"] else "",
                "tier": _string_value(sheet.cell(row_index, grid["tier_col"]).value) if grid["tier_col"] else "",
                "region": _string_value(sheet.cell(row_index, grid["region_col"]).value) if grid["region_col"] else "",
                "delivery_date": _string_value(sheet.cell(row_index, grid["date_col"]).value) if grid["date_col"] else "",
                "weight": round(weight, 2),
                "value": round(value, 2),
                "total_quantity": round(total_quantity, 2),
                "product_count": len(quantities),
                "items": quantities,
                "top_items": quantities[:5],
            }
        )
        row_index += 1

    load_rows, load_totals = _parse_load_list_sheet(load_sheet) if load_sheet is not None else ([], {})
    top_products = _sorted_top_products(active_product_totals, 12)

    return {
        "day_name": day_name,
        "g2g_total": summary_rows["g2g_total"]["total"],
        "loaded_total": summary_rows["loaded_total"]["total"],
        "remaining_total": summary_rows["remaining_total"]["total"],
        "expected_store_total": summary_rows["expected_store_total"]["total"],
        "batch_count": len({row["batch_name"] for row in batch_rows}),
        "active_store_count": len(batch_rows),
        "total_weight": _decimal_or_none(sum(row["weight"] for row in batch_rows)),
        "total_value": _decimal_or_none(sum(row["value"] for row in batch_rows)),
        "store_rows": batch_rows,
        "top_products": top_products,
        "load_rows": load_rows,
        "load_1_total": _decimal_or_none(load_totals.get("load_1", 0.0)),
        "load_2_total": _decimal_or_none(load_totals.get("load_2", 0.0)),
        "load_3_total": _decimal_or_none(load_totals.get("load_3", 0.0)),
        "load_4_total": _decimal_or_none(load_totals.get("load_4", 0.0)),
        "load_total": _decimal_or_none(load_totals.get("total", 0.0)),
    }


def _parse_support_sheet(sheet: Any) -> dict[str, Any]:
    grid = _locate_support_grid(sheet)
    g2g = _parse_metric_row(sheet, grid["product_headers"], PLANNING_SUMMARY_LABELS["g2g_total"])
    loaded = _parse_metric_row(sheet, grid["product_headers"], PLANNING_SUMMARY_LABELS["loaded_total"])
    remaining = _parse_metric_row(sheet, grid["product_headers"], PLANNING_SUMMARY_LABELS["remaining_total"])

    batch_rows = []
    if grid["header_row_index"] is not None and grid["store_col"]:
        current_batch = ""
        row_index = grid["header_row_index"] + 1
        while row_index <= sheet.max_row:
            batch_value = _string_value(sheet.cell(row_index, 1).value)
            if batch_value.lower().startswith("load "):
                current_batch = batch_value
                row_index += 1
                continue
            if _normalize_text(sheet.cell(row_index, grid["store_col"]).value) == "load for external delivery":
                row_index += 1
                continue
            store_name = _string_value(sheet.cell(row_index, grid["store_col"]).value)
            if not store_name:
                row_index += 1
                continue
            items: list[dict[str, Any]] = []
            total_quantity = 0.0
            for column_index, sku_name in grid["product_headers"]:
                quantity = _float_value(sheet.cell(row_index, column_index).value)
                if quantity <= 0:
                    continue
                total_quantity += quantity
                items.append({"sku": sku_name, "quantity": round(quantity, 2)})
            if total_quantity > 0:
                items.sort(key=lambda item: (-item["quantity"], item["sku"]))
                batch_rows.append(
                    {
                        "batch_name": current_batch or "Pending",
                        "store_name": store_name,
                        "contact": _string_value(sheet.cell(row_index, grid["contact_col"]).value) if grid["contact_col"] else "",
                        "region": _string_value(sheet.cell(row_index, grid["region_col"]).value) if grid["region_col"] else "",
                        "value": round(_float_value(sheet.cell(row_index, grid["value_col"]).value), 2)
                        if grid["value_col"]
                        else 0.0,
                        "total_quantity": round(total_quantity, 2),
                        "items": items,
                        "top_items": items[:5],
                        "reason_text": "Imported from Pending Orders",
                    }
                )
            row_index += 1

    return {
        "g2g_total": g2g["total"],
        "loaded_total": loaded["total"],
        "remaining_total": remaining["total"],
        "store_rows": batch_rows,
        "top_products": remaining["top_products"] or g2g["top_products"],
    }


def _parse_inventory_sheet(sheet: Any) -> dict[str, Any]:
    grid = _locate_support_grid(sheet)
    g2g = _parse_metric_row(sheet, grid["product_headers"], PLANNING_SUMMARY_LABELS["g2g_total"])
    remaining = _parse_metric_row(sheet, grid["product_headers"], PLANNING_SUMMARY_LABELS["remaining_total"])
    g2g_items = {item["sku"]: item["quantity"] for item in g2g["items"]}
    remaining_items = {item["sku"]: item["quantity"] for item in remaining["items"]}
    inventory_items = []
    for sort_order, sku_name in enumerate(sorted(set(g2g_items) | set(remaining_items)), start=1):
        inventory_items.append(
            {
                "sku": sku_name,
                "opening_g2g_qty": g2g_items.get(sku_name, 0.0),
                "opening_remaining_qty": remaining_items.get(sku_name, 0.0),
                "sort_order": sort_order,
            }
        )
    return {
        "g2g_total": g2g["total"],
        "remaining_total": remaining["total"],
        "top_products": g2g["top_products"],
        "inventory_items": inventory_items,
    }


def _parse_metric_row(sheet: Any, product_headers: list[tuple[int, str]], label: str) -> dict[str, Any]:
    row_index = _find_row_index(sheet, label)
    if row_index is None:
        return {"total": None, "top_products": [], "items": []}

    items = []
    total = 0.0
    for column_index, sku_name in product_headers:
        quantity = _float_value(sheet.cell(row_index, column_index).value)
        if quantity <= 0:
            continue
        items.append({"sku": sku_name, "quantity": round(quantity, 2)})
        total += quantity

    items.sort(key=lambda item: (-item["quantity"], item["sku"]))
    return {
        "total": _decimal_or_none(total),
        "top_products": items[:10],
        "items": items,
    }


def _parse_assumptions_sheet(sheet: Any) -> dict[str, int]:
    sku_count = 0
    stores = set()
    for row_index in range(2, sheet.max_row + 1):
        sku_name = _string_value(sheet.cell(row_index, 2).value)
        store_name = _string_value(sheet.cell(row_index, 7).value)
        if sku_name:
            sku_count += 1
        if store_name:
            stores.add(store_name)
    return {"sku_count": sku_count, "store_count": len(stores)}


def _parse_fee_sheet(sheet: Any) -> dict[str, Any]:
    header_map = {}
    for column_index in range(1, sheet.max_column + 1):
        header_map[_normalize_text(sheet.cell(1, column_index).value)] = column_index

    sku_col = header_map.get("sku")
    delivery_value_col = header_map.get("retail deliveries value")
    payment_value_col = header_map.get("payment collection value")
    if not sku_col:
        return {"row_count": 0, "total_delivery_value": None, "total_payment_value": None, "top_rows": [], "rows": []}

    rows = []
    total_delivery_value = 0.0
    total_payment_value = 0.0
    for row_index in range(2, sheet.max_row + 1):
        sku_name = _string_value(sheet.cell(row_index, sku_col).value)
        if not sku_name:
            continue
        delivery_value = _float_value(sheet.cell(row_index, delivery_value_col).value) if delivery_value_col else 0.0
        payment_value = _float_value(sheet.cell(row_index, payment_value_col).value) if payment_value_col else 0.0
        total_delivery_value += delivery_value
        total_payment_value += payment_value
        rows.append(
            {
                "brand_partner": _string_value(sheet.cell(row_index, header_map.get("brand partner", 1)).value),
                "sku": sku_name,
                "vatable": _string_value(sheet.cell(row_index, header_map.get("vatable yes no", 1)).value),
                "retail_delivery_value": round(delivery_value, 2),
                "payment_collection_value": round(payment_value, 2),
            }
        )

    rows.sort(key=lambda row: (-row["retail_delivery_value"], row["sku"]))
    return {
        "row_count": len(rows),
        "total_delivery_value": _decimal_or_none(total_delivery_value),
        "total_payment_value": _decimal_or_none(total_payment_value),
        "top_rows": rows[:12],
        "rows": rows,
    }


def _parse_load_list_sheet(sheet: Any) -> tuple[list[dict[str, Any]], dict[str, float]]:
    header_row_index = None
    for row_index in range(1, min(sheet.max_row, 12) + 1):
        if _normalize_text(sheet.cell(row_index, 1).value) == "sku":
            header_row_index = row_index
            break

    if header_row_index is None:
        return [], {}

    rows = []
    totals = {"load_1": 0.0, "load_2": 0.0, "load_3": 0.0, "load_4": 0.0, "total": 0.0}
    for row_index in range(header_row_index + 1, sheet.max_row + 1):
        sku_name = _string_value(sheet.cell(row_index, 1).value)
        if not sku_name:
            continue
        load_1 = _float_value(sheet.cell(row_index, 2).value)
        load_2 = _float_value(sheet.cell(row_index, 3).value)
        load_3 = _float_value(sheet.cell(row_index, 4).value)
        load_4 = _float_value(sheet.cell(row_index, 5).value)
        total = _float_value(sheet.cell(row_index, 6).value)
        if total <= 0 and load_1 <= 0 and load_2 <= 0 and load_3 <= 0 and load_4 <= 0:
            continue
        totals["load_1"] += load_1
        totals["load_2"] += load_2
        totals["load_3"] += load_3
        totals["load_4"] += load_4
        totals["total"] += total
        rows.append(
            {
                "sku": sku_name,
                "load_1": round(load_1, 2),
                "load_2": round(load_2, 2),
                "load_3": round(load_3, 2),
                "load_4": round(load_4, 2),
                "total": round(total, 2),
            }
        )
    rows.sort(key=lambda row: (-row["total"], row["sku"]))
    return rows, totals


def _resolve_fee_sheet(workbook: Any):
    for sheet_name in ("BP NEW FEES", "BP NEW FEES (2)"):
        if sheet_name in workbook.sheetnames:
            return workbook[sheet_name]
    return None


def _locate_day_grid(sheet: Any) -> dict[str, Any]:
    header_row_index = None
    store_col = None
    for row_index in range(1, min(sheet.max_row, 50) + 1):
        for column_index in range(1, min(sheet.max_column, 30) + 1):
            if _normalize_text(sheet.cell(row_index, column_index).value) == "load for external delivery":
                header_row_index = row_index
                store_col = column_index
                break
        if header_row_index is not None:
            break

    if header_row_index is None or store_col is None:
        raise LoadingTrackerError(
            f"We could not find the main planning table in the '{sheet.title}' sheet."
        )

    return _build_grid_definition(sheet, header_row_index, store_col)


def _locate_support_grid(sheet: Any) -> dict[str, Any]:
    try:
        return _locate_day_grid(sheet)
    except LoadingTrackerError:
        product_header_row = _find_row_index(sheet, "PRODUCTS DESCRIPTIONS") or 2
        product_headers = []
        for column_index in range(10, sheet.max_column + 1):
            sku_name = _string_value(sheet.cell(product_header_row, column_index).value)
            if sku_name:
                product_headers.append((column_index, sku_name))
        return {
            "header_row_index": None,
            "store_col": None,
            "contact_col": None,
            "lp_col": None,
            "tier_col": None,
            "region_col": None,
            "weight_col": None,
            "value_col": None,
            "date_col": None,
            "product_headers": product_headers,
        }


def _build_grid_definition(sheet: Any, header_row_index: int, store_col: int) -> dict[str, Any]:
    header_values = {
        _normalize_text(sheet.cell(header_row_index, column_index).value): column_index
        for column_index in range(1, min(sheet.max_column, store_col + 20) + 1)
    }
    product_headers = []
    for column_index in range(store_col + 1, sheet.max_column + 1):
        sku_name = _string_value(sheet.cell(header_row_index, column_index).value)
        if sku_name:
            product_headers.append((column_index, sku_name))

    return {
        "header_row_index": header_row_index,
        "store_col": store_col,
        "contact_col": header_values.get("contact"),
        "lp_col": header_values.get("lp"),
        "tier_col": header_values.get("tier"),
        "region_col": header_values.get("region"),
        "weight_col": header_values.get("weight"),
        "value_col": header_values.get("value"),
        "date_col": header_values.get("date"),
        "product_headers": product_headers,
    }


def _find_row_index(sheet: Any, label: str) -> int | None:
    target = _normalize_text(label)
    for row_index in range(1, min(sheet.max_row, 40) + 1):
        for column_index in range(1, min(sheet.max_column, 12) + 1):
            if _normalize_text(sheet.cell(row_index, column_index).value) == target:
                return row_index
    return None


def _count_note_lines(sheet: Any) -> int:
    count = 0
    for row_index in range(1, sheet.max_row + 1):
        values = [_string_value(sheet.cell(row_index, column_index).value) for column_index in range(1, sheet.max_column + 1)]
        if any(values):
            count += 1
    return count


def _sorted_top_products(product_totals: dict[str, float], limit: int) -> list[dict[str, Any]]:
    ranked = sorted(product_totals.items(), key=lambda item: (-item[1], item[0]))
    return [{"sku": sku, "quantity": round(quantity, 2)} for sku, quantity in ranked[:limit]]


def _batch_sort_key(batch_name: str) -> tuple[int, str]:
    label = batch_name.lower().replace("load", "").strip()
    try:
        return int(label), batch_name
    except ValueError:
        return 999, batch_name


def _load_workbook_from_upload(file_storage: Any):
    payload = file_storage.read()
    file_storage.stream.seek(0)
    try:
        return load_workbook(BytesIO(payload), data_only=True)
    except Exception as exc:  # pragma: no cover
        raise LoadingTrackerError("The uploaded file could not be read as an Excel workbook.") from exc


def _string_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_text(value: Any) -> str:
    return " ".join(_string_value(value).lower().split())


def _float_value(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return float(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return 0.0


def _decimal_value(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def _decimal_or_none(value: float | Decimal | None) -> Decimal | None:
    if value in (None, ""):
        return None
    value_float = _float_value(value)
    if abs(value_float) < 0.000001:
        return None
    return Decimal(f"{value_float:.4f}")


def _clean_filename_part(value: str, fallback: str) -> str:
    import re

    cleaned = re.sub(INVALID_FILENAME_CHARS, " ", str(value))
    cleaned = cleaned.replace("_", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .-")
    if not cleaned:
        return fallback
    return cleaned[:90].rstrip(" .-")
