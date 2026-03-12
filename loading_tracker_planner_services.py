from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

from sqlalchemy import delete, select

from loading_tracker_services import (
    DAY_SHEET_NAMES,
    LoadingTrackerError,
    get_loading_tracker_day,
    get_loading_tracker_import,
    get_loading_tracker_template,
    import_loading_tracker_payload,
)
from models import (
    LoadingTrackerCarryForwardItem,
    LoadingTrackerCountEntry,
    LoadingTrackerImport,
    LoadingTrackerNotification,
    LoadingTrackerPlanEvent,
    LoadingTrackerPlanLine,
    Product,
    db,
)

PENDING_REASON_OPTIONS = [
    "Out of stock",
    "Receiving day restriction",
    "Retailer postponement",
    "Route issue",
    "Credit hold",
    "Planner decision",
    "Damaged / short stock",
    "Other",
]
LOAD_BATCHES = ["Load 1", "Load 2", "Load 3", "Load 4"]


@dataclass
class LoadingTrackerDayWorkspace:
    tracker_import: LoadingTrackerImport
    day: Any
    planned_lines: list[LoadingTrackerPlanLine]
    pending_lines: list[LoadingTrackerPlanLine]
    count_entries: list[LoadingTrackerCountEntry]
    notifications: list[LoadingTrackerNotification]
    ll_rows: list[dict[str, Any]]
    ll_totals: dict[str, Decimal]
    pack_breaker_rows: list[dict[str, Any]]
    summary: dict[str, Decimal | int]


@dataclass
class LoadingTrackerPendingWorkspace:
    tracker_import: LoadingTrackerImport
    pending_lines: list[LoadingTrackerPlanLine]
    notifications: list[LoadingTrackerNotification]
    reason_options: list[str]
    total_pending_quantity: Decimal


@dataclass
class LoadingTrackerInventoryWorkspace:
    tracker_import: LoadingTrackerImport
    carry_forward_items: list[LoadingTrackerCarryForwardItem]
    counts_by_day: dict[str, list[LoadingTrackerCountEntry]]
    notifications: list[LoadingTrackerNotification]


def ensure_loading_tracker_planner_initialized(tracker_import: LoadingTrackerImport) -> None:
    existing = db.session.scalar(
        select(LoadingTrackerPlanLine.id).where(LoadingTrackerPlanLine.tracker_import_id == tracker_import.id).limit(1)
    )
    if existing is not None:
        return

    sort_order = 0
    for day in tracker_import.days:
        for row in day.store_rows_json or []:
            items = row.get("items") or row.get("top_items") or []
            total_quantity = sum(float(item.get("quantity", 0) or 0) for item in items) or 0.0
            row_value = _decimal_value(row.get("value")) or Decimal("0")
            row_weight = _decimal_value(row.get("weight")) or Decimal("0")
            for item in items:
                sort_order += 1
                quantity = _decimal_value(item.get("quantity")) or Decimal("0")
                ratio = (quantity / Decimal(str(total_quantity))) if total_quantity else Decimal("0")
                db.session.add(
                    LoadingTrackerPlanLine(
                        tracker_import_id=tracker_import.id,
                        day_name=day.day_name,
                        original_day_name=day.day_name,
                        line_status="planned",
                        batch_name=row.get("batch_name") or "Unassigned",
                        store_name=row.get("store_name") or "",
                        normalized_store_name=_normalize_text(row.get("store_name")),
                        sku_name=item.get("sku") or "",
                        normalized_sku_name=_normalize_text(item.get("sku")),
                        quantity=quantity,
                        original_quantity=quantity,
                        value=_quantize_or_none(row_value * ratio),
                        weight=_quantize_or_none(row_weight * ratio),
                        contact=row.get("contact") or "",
                        lp=row.get("lp") or "",
                        tier=row.get("tier") or "",
                        region=row.get("region") or "",
                        delivery_date=row.get("delivery_date") or "",
                        sort_order=sort_order,
                    )
                )

        if not db.session.scalar(
            select(LoadingTrackerCountEntry.id)
            .where(LoadingTrackerCountEntry.tracker_import_id == tracker_import.id, LoadingTrackerCountEntry.day_name == day.day_name)
            .limit(1)
        ):
            for item in day.top_products_json or []:
                quantity = _decimal_value(item.get("quantity")) or Decimal("0")
                db.session.add(
                    LoadingTrackerCountEntry(
                        tracker_import_id=tracker_import.id,
                        day_name=day.day_name,
                        sku_name=item.get("sku") or "",
                        normalized_sku_name=_normalize_text(item.get("sku")),
                        expected_quantity=quantity,
                        physical_quantity=quantity,
                        discrepancy_quantity=Decimal("0"),
                    )
                )

    for row in tracker_import.pending_rows_json or []:
        items = row.get("items") or row.get("top_items") or []
        total_quantity = sum(float(item.get("quantity", 0) or 0) for item in items) or 0.0
        row_value = _decimal_value(row.get("value")) or Decimal("0")
        for item in items:
            sort_order += 1
            quantity = _decimal_value(item.get("quantity")) or Decimal("0")
            ratio = (quantity / Decimal(str(total_quantity))) if total_quantity else Decimal("0")
            db.session.add(
                LoadingTrackerPlanLine(
                    tracker_import_id=tracker_import.id,
                    day_name="Pending",
                    original_day_name="Pending",
                    line_status="pending",
                    batch_name=row.get("batch_name") or "Unassigned",
                    store_name=row.get("store_name") or "",
                    normalized_store_name=_normalize_text(row.get("store_name")),
                    sku_name=item.get("sku") or "",
                    normalized_sku_name=_normalize_text(item.get("sku")),
                    quantity=quantity,
                    original_quantity=quantity,
                    value=_quantize_or_none(row_value * ratio),
                    weight=None,
                    contact=row.get("contact") or "",
                    region=row.get("region") or "",
                    reason_code="Imported pending",
                    reason_note="Planner records initialized from an older imported week.",
                    sort_order=sort_order,
                )
            )

    db.session.commit()


def build_loading_tracker_day_workspace(import_id: str, day_name: str) -> LoadingTrackerDayWorkspace:
    tracker_import = get_loading_tracker_import(import_id)
    if tracker_import is None:
        raise LoadingTrackerError("That loading-tracker week could not be found.")
    ensure_loading_tracker_planner_initialized(tracker_import)
    day = get_loading_tracker_day(import_id, day_name)
    if day is None:
        raise LoadingTrackerError("That planning day could not be found.")

    planned_lines = list(
        db.session.scalars(
            select(LoadingTrackerPlanLine)
            .where(
                LoadingTrackerPlanLine.tracker_import_id == import_id,
                LoadingTrackerPlanLine.day_name == day_name,
                LoadingTrackerPlanLine.line_status == "planned",
            )
            .order_by(
                LoadingTrackerPlanLine.batch_name.asc(),
                LoadingTrackerPlanLine.store_name.asc(),
                LoadingTrackerPlanLine.sku_name.asc(),
                LoadingTrackerPlanLine.id.asc(),
            )
        )
    )
    pending_lines = list(
        db.session.scalars(
            select(LoadingTrackerPlanLine)
            .where(
                LoadingTrackerPlanLine.tracker_import_id == import_id,
                LoadingTrackerPlanLine.line_status == "pending",
            )
            .order_by(LoadingTrackerPlanLine.store_name.asc(), LoadingTrackerPlanLine.sku_name.asc())
            .limit(24)
        )
    )
    count_entries = list(
        db.session.scalars(
            select(LoadingTrackerCountEntry)
            .where(
                LoadingTrackerCountEntry.tracker_import_id == import_id,
                LoadingTrackerCountEntry.day_name == day_name,
            )
            .order_by(LoadingTrackerCountEntry.sku_name.asc())
        )
    )
    notifications = list(
        db.session.scalars(
            select(LoadingTrackerNotification)
            .where(
                LoadingTrackerNotification.tracker_import_id == import_id,
                (LoadingTrackerNotification.day_name == day_name) | (LoadingTrackerNotification.day_name.is_(None)),
            )
            .order_by(LoadingTrackerNotification.created_at.desc())
            .limit(12)
        )
    )
    ll_rows, ll_totals = build_loading_list_from_plan_lines(planned_lines)
    pack_breaker_rows = build_pack_breaker_from_plan_lines(planned_lines)
    physical_total = sum((entry.physical_quantity or Decimal("0")) for entry in count_entries)
    expected_total = sum((entry.expected_quantity or Decimal("0")) for entry in count_entries)
    planned_total = sum((line.quantity or Decimal("0")) for line in planned_lines)
    pending_total = sum((line.quantity or Decimal("0")) for line in pending_lines)

    return LoadingTrackerDayWorkspace(
        tracker_import=tracker_import,
        day=day,
        planned_lines=planned_lines,
        pending_lines=pending_lines,
        count_entries=count_entries,
        notifications=notifications,
        ll_rows=ll_rows,
        ll_totals=ll_totals,
        pack_breaker_rows=pack_breaker_rows,
        summary={
            "physical_total": physical_total,
            "expected_total": expected_total,
            "planned_total": planned_total,
            "pending_total": pending_total,
            "projected_close": physical_total - planned_total,
            "planned_line_count": len(planned_lines),
        },
    )


def build_loading_tracker_pending_workspace(import_id: str) -> LoadingTrackerPendingWorkspace:
    tracker_import = get_loading_tracker_import(import_id)
    if tracker_import is None:
        raise LoadingTrackerError("That loading-tracker week could not be found.")
    ensure_loading_tracker_planner_initialized(tracker_import)
    pending_lines = list(
        db.session.scalars(
            select(LoadingTrackerPlanLine)
            .where(
                LoadingTrackerPlanLine.tracker_import_id == import_id,
                LoadingTrackerPlanLine.line_status == "pending",
            )
            .order_by(LoadingTrackerPlanLine.batch_name.asc(), LoadingTrackerPlanLine.store_name.asc(), LoadingTrackerPlanLine.sku_name.asc())
        )
    )
    notifications = list(
        db.session.scalars(
            select(LoadingTrackerNotification)
            .where(LoadingTrackerNotification.tracker_import_id == import_id)
            .order_by(LoadingTrackerNotification.created_at.desc())
            .limit(12)
        )
    )
    total_pending_quantity = sum((line.quantity or Decimal("0")) for line in pending_lines)
    return LoadingTrackerPendingWorkspace(
        tracker_import=tracker_import,
        pending_lines=pending_lines,
        notifications=notifications,
        reason_options=PENDING_REASON_OPTIONS,
        total_pending_quantity=total_pending_quantity,
    )


def build_loading_tracker_inventory_workspace(import_id: str) -> LoadingTrackerInventoryWorkspace:
    tracker_import = get_loading_tracker_import(import_id)
    if tracker_import is None:
        raise LoadingTrackerError("That loading-tracker week could not be found.")
    ensure_loading_tracker_planner_initialized(tracker_import)
    carry_forward_items = list(
        db.session.scalars(
            select(LoadingTrackerCarryForwardItem)
            .where(LoadingTrackerCarryForwardItem.tracker_import_id == import_id)
            .order_by(LoadingTrackerCarryForwardItem.sku_name.asc())
        )
    )
    counts = list(
        db.session.scalars(
            select(LoadingTrackerCountEntry)
            .where(LoadingTrackerCountEntry.tracker_import_id == import_id)
            .order_by(LoadingTrackerCountEntry.day_name.asc(), LoadingTrackerCountEntry.sku_name.asc())
        )
    )
    counts_by_day: dict[str, list[LoadingTrackerCountEntry]] = {day_name: [] for day_name in DAY_SHEET_NAMES}
    for entry in counts:
        counts_by_day.setdefault(entry.day_name, []).append(entry)
    notifications = list(
        db.session.scalars(
            select(LoadingTrackerNotification)
            .where(LoadingTrackerNotification.tracker_import_id == import_id)
            .order_by(LoadingTrackerNotification.created_at.desc())
            .limit(20)
        )
    )
    return LoadingTrackerInventoryWorkspace(
        tracker_import=tracker_import,
        carry_forward_items=carry_forward_items,
        counts_by_day=counts_by_day,
        notifications=notifications,
    )


def save_loading_tracker_counts(import_id: str, day_name: str, form_data: dict[str, Any]) -> None:
    tracker_import = get_loading_tracker_import(import_id)
    if tracker_import is None:
        raise LoadingTrackerError("That loading-tracker week could not be found.")
    ensure_loading_tracker_planner_initialized(tracker_import)
    count_entries = list(
        db.session.scalars(
            select(LoadingTrackerCountEntry)
            .where(
                LoadingTrackerCountEntry.tracker_import_id == import_id,
                LoadingTrackerCountEntry.day_name == day_name,
            )
        )
    )
    if not count_entries:
        raise LoadingTrackerError("No expected count rows were available for this day.")

    db.session.execute(
        delete(LoadingTrackerNotification).where(
            LoadingTrackerNotification.tracker_import_id == import_id,
            LoadingTrackerNotification.day_name == day_name,
            LoadingTrackerNotification.kind.in_(["inventory-shortage", "inventory-surplus"]),
        )
    )

    shortage_count = 0
    surplus_count = 0
    for entry in count_entries:
        raw_value = form_data.get(f"physical::{entry.id}", "")
        physical = _decimal_value(raw_value)
        if physical is None:
            physical = entry.physical_quantity or Decimal("0")
        entry.physical_quantity = physical
        entry.discrepancy_quantity = (physical - (entry.expected_quantity or Decimal("0"))).quantize(
            Decimal("0.0001"),
            rounding=ROUND_HALF_UP,
        )
        if entry.discrepancy_quantity < 0:
            shortage_count += 1
            db.session.add(
                LoadingTrackerNotification(
                    tracker_import_id=import_id,
                    day_name=day_name,
                    kind="inventory-shortage",
                    title=f"Shortage flagged for {entry.sku_name}",
                    body=(
                        f"Morning physical count is {entry.physical_quantity} against expected {entry.expected_quantity}. "
                        f"Shortfall: {abs(entry.discrepancy_quantity)} cartons."
                    ),
                )
            )
        elif entry.discrepancy_quantity > 0:
            surplus_count += 1
            db.session.add(
                LoadingTrackerNotification(
                    tracker_import_id=import_id,
                    day_name=day_name,
                    kind="inventory-surplus",
                    title=f"Surplus flagged for {entry.sku_name}",
                    body=(
                        f"Morning physical count is {entry.physical_quantity} against expected {entry.expected_quantity}. "
                        f"Extra stock: {entry.discrepancy_quantity} cartons."
                    ),
                )
            )

    db.session.add(
        LoadingTrackerPlanEvent(
            tracker_import_id=import_id,
            day_name=day_name,
            action_type="counts_saved",
            note=f"Morning count saved with {shortage_count} shortage(s) and {surplus_count} surplus(es).",
        )
    )
    db.session.commit()


def update_loading_tracker_line(import_id: str, day_name: str, line_id: int, quantity_value: Any) -> None:
    tracker_import = get_loading_tracker_import(import_id)
    if tracker_import is None:
        raise LoadingTrackerError("That loading-tracker week could not be found.")
    line = db.session.get(LoadingTrackerPlanLine, line_id)
    if line is None or line.tracker_import_id != import_id:
        raise LoadingTrackerError("That plan line could not be found.")

    quantity = _decimal_value(quantity_value)
    if quantity is None or quantity < 0:
        raise LoadingTrackerError("Quantity must be zero or greater.")
    old_quantity = line.quantity or Decimal("0")
    line.quantity = quantity
    line.day_name = day_name
    line.line_status = "planned"
    db.session.add(
        LoadingTrackerPlanEvent(
            tracker_import_id=import_id,
            day_name=day_name,
            line_id=line.id,
            action_type="quantity_updated",
            store_name=line.store_name,
            sku_name=line.sku_name,
            old_quantity=old_quantity,
            new_quantity=quantity,
        )
    )
    db.session.commit()


def move_loading_tracker_line_to_pending(import_id: str, day_name: str, line_id: int, reason_code: str, note: str) -> None:
    line = db.session.get(LoadingTrackerPlanLine, line_id)
    if line is None or line.tracker_import_id != import_id:
        raise LoadingTrackerError("That plan line could not be found.")
    reason = (reason_code or "").strip()
    if reason not in PENDING_REASON_OPTIONS:
        raise LoadingTrackerError("Choose a valid pending reason.")

    old_day_name = line.day_name
    line.day_name = "Pending"
    line.line_status = "pending"
    line.reason_code = reason
    line.reason_note = (note or "").strip()
    db.session.add(
        LoadingTrackerPlanEvent(
            tracker_import_id=import_id,
            day_name=day_name,
            line_id=line.id,
            action_type="moved_to_pending",
            store_name=line.store_name,
            sku_name=line.sku_name,
            old_quantity=line.quantity,
            new_quantity=line.quantity,
            reason_code=reason,
            note=f"Moved from {old_day_name} to pending. {line.reason_note}".strip(),
        )
    )
    db.session.add(
        LoadingTrackerNotification(
            tracker_import_id=import_id,
            day_name=day_name,
            kind="pending-move",
            title=f"{line.store_name} / {line.sku_name} moved to pending",
            body=f"Reason: {reason}. {line.reason_note}".strip(),
        )
    )
    db.session.commit()


def return_loading_tracker_pending_line(import_id: str, line_id: int, target_day_name: str) -> None:
    line = db.session.get(LoadingTrackerPlanLine, line_id)
    if line is None or line.tracker_import_id != import_id:
        raise LoadingTrackerError("That pending line could not be found.")
    if target_day_name not in DAY_SHEET_NAMES:
        raise LoadingTrackerError("Choose a valid day to return this line to.")

    line.day_name = target_day_name
    line.line_status = "planned"
    previous_reason = line.reason_code or ""
    previous_note = line.reason_note or ""
    line.reason_code = None
    line.reason_note = None
    db.session.add(
        LoadingTrackerPlanEvent(
            tracker_import_id=import_id,
            day_name=target_day_name,
            line_id=line.id,
            action_type="returned_from_pending",
            store_name=line.store_name,
            sku_name=line.sku_name,
            old_quantity=line.quantity,
            new_quantity=line.quantity,
            reason_code=previous_reason,
            note=previous_note,
        )
    )
    db.session.commit()


def add_loading_tracker_manual_line(import_id: str, day_name: str, form_data: dict[str, Any]) -> None:
    tracker_import = get_loading_tracker_import(import_id)
    if tracker_import is None:
        raise LoadingTrackerError("That loading-tracker week could not be found.")
    if day_name not in DAY_SHEET_NAMES:
        raise LoadingTrackerError("Choose a valid day first.")

    store_name = (form_data.get("store_name") or "").strip()
    sku_name = (form_data.get("sku_name") or "").strip()
    quantity = _decimal_value(form_data.get("quantity"))
    if not store_name or not sku_name:
        raise LoadingTrackerError("Store name and SKU are required for a manual line.")
    if quantity is None or quantity <= 0:
        raise LoadingTrackerError("Manual quantity must be greater than zero.")

    sort_order = (
        db.session.scalar(
            select(LoadingTrackerPlanLine.sort_order)
            .where(LoadingTrackerPlanLine.tracker_import_id == import_id)
            .order_by(LoadingTrackerPlanLine.sort_order.desc())
            .limit(1)
        )
        or 0
    ) + 1

    line = LoadingTrackerPlanLine(
        tracker_import_id=import_id,
        day_name=day_name,
        original_day_name=day_name,
        line_status="planned",
        batch_name=(form_data.get("batch_name") or "Unassigned").strip() or "Unassigned",
        store_name=store_name,
        normalized_store_name=_normalize_text(store_name),
        sku_name=sku_name,
        normalized_sku_name=_normalize_text(sku_name),
        quantity=quantity,
        original_quantity=quantity,
        contact=(form_data.get("contact") or "").strip(),
        lp=(form_data.get("lp") or "").strip(),
        tier=(form_data.get("tier") or "").strip(),
        region=(form_data.get("region") or "").strip(),
        delivery_date=(form_data.get("delivery_date") or "").strip(),
        is_manual=True,
        sort_order=sort_order,
    )
    db.session.add(line)
    db.session.flush()
    db.session.add(
        LoadingTrackerPlanEvent(
            tracker_import_id=import_id,
            day_name=day_name,
            line_id=line.id,
            action_type="manual_line_added",
            store_name=line.store_name,
            sku_name=line.sku_name,
            old_quantity=Decimal("0"),
            new_quantity=line.quantity,
        )
    )
    db.session.commit()


def build_loading_list_from_plan_lines(lines: list[LoadingTrackerPlanLine]) -> tuple[list[dict[str, Any]], dict[str, Decimal]]:
    grouped: dict[str, dict[str, Decimal]] = defaultdict(lambda: {key: Decimal("0") for key in ["load_1", "load_2", "load_3", "load_4", "total"]})
    totals = {key: Decimal("0") for key in ["load_1", "load_2", "load_3", "load_4", "total"]}
    for line in lines:
        sku = line.sku_name
        quantity = line.quantity or Decimal("0")
        batch_key = _load_key(line.batch_name)
        grouped[sku][batch_key] += quantity
        grouped[sku]["total"] += quantity
        totals[batch_key] += quantity
        totals["total"] += quantity

    rows = []
    for sku in sorted(grouped):
        values = grouped[sku]
        rows.append(
            {
                "sku": sku,
                "load_1": values["load_1"],
                "load_2": values["load_2"],
                "load_3": values["load_3"],
                "load_4": values["load_4"],
                "total": values["total"],
            }
        )
    rows.sort(key=lambda item: (-(item["total"] or Decimal("0")), item["sku"]))
    return rows, totals


def build_pack_breaker_from_plan_lines(lines: list[LoadingTrackerPlanLine]) -> list[dict[str, Any]]:
    sku_totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for line in lines:
        sku_totals[line.sku_name] += line.quantity or Decimal("0")

    products = {
        product.sku_name: product
        for product in db.session.scalars(select(Product).where(Product.sku_name.in_(list(sku_totals.keys()))))
    }
    rows: list[dict[str, Any]] = []
    for sku_name, total_quantity in sku_totals.items():
        fractional = total_quantity - Decimal(int(total_quantity))
        quarters = int((fractional * Decimal("4")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        if quarters <= 0:
            continue
        half_packs = quarters // 2
        quarter_packs = quarters % 2
        conversion = products.get(sku_name).conversion if products.get(sku_name) is not None else None
        if conversion is not None:
            conversion_decimal = conversion if isinstance(conversion, Decimal) else Decimal(str(conversion))
            broken_pieces = (Decimal(half_packs) * conversion_decimal / Decimal("2")) + (
                Decimal(quarter_packs) * conversion_decimal / Decimal("4")
            )
        else:
            broken_pieces = Decimal(half_packs) * Decimal("0.5") + Decimal(quarter_packs) * Decimal("0.25")
        rows.append(
            {
                "sku": sku_name,
                "half_packs": half_packs,
                "quarter_packs": quarter_packs,
                "broken_pieces": broken_pieces.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            }
        )
    rows.sort(key=lambda item: (-item["broken_pieces"], item["sku"]))
    return rows


def start_new_loading_tracker_week() -> LoadingTrackerImport:
    template = get_loading_tracker_template()
    if template is None or not template.workbook_bytes:
        raise LoadingTrackerError("Save a backend loading-tracker template before starting a new week.")

    previous_import = get_loading_tracker_import()
    new_import = import_loading_tracker_payload(
        template.workbook_bytes,
        template.source_filename or "loading-tracker-template.xlsx",
        week_label=f"{template.name} - New Week",
    )

    if previous_import is not None and previous_import.id != new_import.id:
        ensure_loading_tracker_planner_initialized(previous_import)
        pending_lines = list(
            db.session.scalars(
                select(LoadingTrackerPlanLine).where(
                    LoadingTrackerPlanLine.tracker_import_id == previous_import.id,
                    LoadingTrackerPlanLine.line_status == "pending",
                )
            )
        )
        for line in pending_lines:
            db.session.add(
                LoadingTrackerPlanLine(
                    tracker_import_id=new_import.id,
                    day_name="Pending",
                    original_day_name="Pending",
                    line_status="pending",
                    batch_name=line.batch_name,
                    store_name=line.store_name,
                    normalized_store_name=line.normalized_store_name,
                    sku_name=line.sku_name,
                    normalized_sku_name=line.normalized_sku_name,
                    quantity=line.quantity,
                    original_quantity=line.quantity,
                    value=line.value,
                    weight=line.weight,
                    contact=line.contact,
                    lp=line.lp,
                    tier=line.tier,
                    region=line.region,
                    delivery_date=line.delivery_date,
                    reason_code=line.reason_code or "Carry forward pending",
                    reason_note=line.reason_note or "Moved into the new week during carry-forward.",
                    is_manual=line.is_manual,
                    sort_order=line.sort_order,
                )
            )

        carry_map = _build_carry_forward_inventory(previous_import.id)
        for sku_name, quantity in carry_map.items():
            db.session.add(
                LoadingTrackerCarryForwardItem(
                    tracker_import_id=new_import.id,
                    source_import_id=previous_import.id,
                    sku_name=sku_name,
                    normalized_sku_name=_normalize_text(sku_name),
                    quantity=quantity,
                )
            )
            monday_entry = db.session.scalar(
                select(LoadingTrackerCountEntry).where(
                    LoadingTrackerCountEntry.tracker_import_id == new_import.id,
                    LoadingTrackerCountEntry.day_name == "Mon",
                    LoadingTrackerCountEntry.sku_name == sku_name,
                )
            )
            if monday_entry is None:
                monday_entry = LoadingTrackerCountEntry(
                    tracker_import_id=new_import.id,
                    day_name="Mon",
                    sku_name=sku_name,
                    normalized_sku_name=_normalize_text(sku_name),
                    expected_quantity=quantity,
                    physical_quantity=quantity,
                    discrepancy_quantity=Decimal("0"),
                )
                db.session.add(monday_entry)
            else:
                monday_entry.expected_quantity = (monday_entry.expected_quantity or Decimal("0")) + quantity
                monday_entry.physical_quantity = (monday_entry.physical_quantity or Decimal("0")) + quantity
                monday_entry.discrepancy_quantity = Decimal("0")

        db.session.add(
            LoadingTrackerNotification(
                tracker_import_id=new_import.id,
                day_name=None,
                kind="carry-forward",
                title="New week created from template",
                body=(
                    f"{len(pending_lines)} pending line(s) and {len(carry_map)} carry-forward stock row(s) were moved into the new week."
                ),
            )
        )

    db.session.commit()
    return new_import


def _build_carry_forward_inventory(import_id: str) -> dict[str, Decimal]:
    latest_day_name = None
    for day_name in reversed(DAY_SHEET_NAMES):
        exists = db.session.scalar(
            select(LoadingTrackerCountEntry.id).where(
                LoadingTrackerCountEntry.tracker_import_id == import_id,
                LoadingTrackerCountEntry.day_name == day_name,
            ).limit(1)
        )
        if exists is not None:
            latest_day_name = day_name
            break

    if latest_day_name is None:
        return {}

    counts = list(
        db.session.scalars(
            select(LoadingTrackerCountEntry).where(
                LoadingTrackerCountEntry.tracker_import_id == import_id,
                LoadingTrackerCountEntry.day_name == latest_day_name,
            )
        )
    )
    planned_lines = list(
        db.session.scalars(
            select(LoadingTrackerPlanLine).where(
                LoadingTrackerPlanLine.tracker_import_id == import_id,
                LoadingTrackerPlanLine.day_name == latest_day_name,
                LoadingTrackerPlanLine.line_status == "planned",
            )
        )
    )
    planned_by_sku: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for line in planned_lines:
        planned_by_sku[line.sku_name] += line.quantity or Decimal("0")

    carry_map: dict[str, Decimal] = {}
    for entry in counts:
        remaining = (entry.physical_quantity or Decimal("0")) - planned_by_sku.get(entry.sku_name, Decimal("0"))
        if remaining > 0:
            carry_map[entry.sku_name] = remaining.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    return carry_map


def _load_key(batch_name: str | None) -> str:
    label = (batch_name or "").strip().lower()
    if label == "load 1":
        return "load_1"
    if label == "load 2":
        return "load_2"
    if label == "load 3":
        return "load_3"
    if label == "load 4":
        return "load_4"
    return "load_4"


def _decimal_value(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).lower().split())


def _quantize_or_none(value: Decimal) -> Decimal | None:
    if value == 0:
        return None
    return value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
