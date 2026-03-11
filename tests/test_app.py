from __future__ import annotations

from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

from openpyxl import Workbook

from app import _database_uri, create_app
from models import (
    LoadingTrackerDailyCount,
    LoadingTrackerDay,
    LoadingTrackerEvent,
    LoadingTrackerImport,
    LoadingTrackerRow,
    Product,
    ProductAlias,
    UploadRun,
    db,
)
from services import bootstrap_seed_uom_if_empty


def build_test_workbook(sheet_name: str = "tracker") -> BytesIO:
    workbook = Workbook()
    tracker = workbook.active
    tracker.title = sheet_name
    tracker.append(["Sales Order Number", "Stores", "SKU Alpha", "SKU Vanila"])
    tracker.append(["SO-100", "Market One", 2, 0.5])
    tracker.append(["SO-101", "Market Two", 1, 0])

    delivery = workbook.create_sheet("Delivery Invoice")
    delivery.append(
        [
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
    )

    uom = workbook.create_sheet("UOM")
    uom.append(["ITEM", "UOM", "ALT UOM", "Conversion", "Vatable", "Prices"])
    uom.append(["SKU Alpha", "ctn", "unt", 12, "Yes", 100])
    uom.append(["SKU Vanilla", "ctn", "unt", 12, "No", 80])

    stream = BytesIO()
    workbook.save(stream)
    stream.seek(0)
    return stream


def build_item_list_export() -> BytesIO:
    payload = (
        "Item Id\tItem Name \t Cases Size \t Tax Rate\t Item PTR\t Status\n"
        "1\tExisting SKU Alpha\t12\t7.5\t999\tActive\n"
        "2\tFresh SKU Beta\t24\t\t3450\tActive\n"
        "3\tRetired SKU Gamma\t6\t\t1200\tInactive\n"
    )
    return BytesIO(payload.encode("utf-8"))


def build_loading_tracker_workbook() -> BytesIO:
    workbook = Workbook()
    workbook.remove(workbook.active)

    def populate_day_sheet(sheet_name: str, store_name: str, qty_a: float, qty_b: float) -> None:
        sheet = workbook.create_sheet(sheet_name)
        sheet.cell(5, 9, "Expected in G2G For Loading")
        sheet.cell(5, 10, 10)
        sheet.cell(5, 11, 4)
        sheet.cell(6, 9, "TOTAL LOADED OUT FOR DELIVERY")
        sheet.cell(6, 10, qty_a)
        sheet.cell(6, 11, qty_b)
        sheet.cell(7, 9, "Remaining Inventory After Loading")
        sheet.cell(7, 10, 10 - qty_a)
        sheet.cell(7, 11, 4 - qty_b)
        sheet.cell(21, 9, "Expected in Store For Loading")
        sheet.cell(21, 10, 10)
        sheet.cell(21, 11, 4)
        sheet.cell(23, 1, "Load 1")
        sheet.cell(24, 2, "Contact")
        sheet.cell(24, 3, "LP")
        sheet.cell(24, 4, "Tier")
        sheet.cell(24, 5, "Region")
        sheet.cell(24, 6, "Weight")
        sheet.cell(24, 7, "Value")
        sheet.cell(24, 8, "Date")
        sheet.cell(24, 9, "LOAD FOR EXTERNAL DELIVERY")
        sheet.cell(24, 10, "SKU Alpha")
        sheet.cell(24, 11, "SKU Beta")
        sheet.cell(25, 2, "Area One")
        sheet.cell(25, 3, "Mr Tester")
        sheet.cell(25, 4, "Tier-1")
        sheet.cell(25, 5, "Reg 1")
        sheet.cell(25, 6, 5.5)
        sheet.cell(25, 7, 25000)
        sheet.cell(25, 9, store_name)
        sheet.cell(25, 10, qty_a)
        sheet.cell(25, 11, qty_b)
        sheet.cell(65, 1, "Load 2")
        sheet.cell(66, 2, "Contact")
        sheet.cell(66, 3, "LP")
        sheet.cell(66, 4, "Tier")
        sheet.cell(66, 5, "Region")
        sheet.cell(66, 6, "Weight")
        sheet.cell(66, 7, "Value")
        sheet.cell(66, 8, "Date")
        sheet.cell(66, 9, "LOAD FOR EXTERNAL DELIVERY")
        sheet.cell(67, 2, "Area Two")
        sheet.cell(67, 3, "Mr Runner")
        sheet.cell(67, 4, "Tier-2")
        sheet.cell(67, 5, "Reg 2")
        sheet.cell(67, 6, 2.0)
        sheet.cell(67, 7, 11000)
        sheet.cell(67, 9, "Second Store")
        sheet.cell(67, 10, 1)
        sheet.cell(67, 11, 0)

    def populate_load_list(sheet_name: str, qty_a: float, qty_b: float) -> None:
        sheet = workbook.create_sheet(sheet_name)
        sheet.cell(1, 1, sheet_name)
        sheet.cell(2, 1, "SKU")
        sheet.cell(2, 2, "Load 1")
        sheet.cell(2, 3, "Load 2")
        sheet.cell(2, 4, "Load 3")
        sheet.cell(2, 5, "Load 4")
        sheet.cell(2, 6, "TOTAL")
        sheet.cell(3, 1, "SKU Alpha")
        sheet.cell(3, 2, qty_a)
        sheet.cell(3, 6, qty_a)
        sheet.cell(4, 1, "SKU Beta")
        sheet.cell(4, 3, qty_b)
        sheet.cell(4, 6, qty_b)

    workbook.create_sheet("Pending Orders")
    pending = workbook["Pending Orders"]
    pending.cell(5, 9, "Expected in G2G For Loading")
    pending.cell(5, 10, 7)
    pending.cell(6, 9, "TOTAL LOADED OUT FOR DELIVERY")
    pending.cell(6, 10, 2)
    pending.cell(7, 9, "Remaining Inventory After Loading")
    pending.cell(7, 10, 5)
    pending.cell(23, 1, "Load 1")
    pending.cell(24, 2, "Contact")
    pending.cell(24, 5, "Region")
    pending.cell(24, 7, "Value")
    pending.cell(24, 9, "LOAD FOR EXTERNAL DELIVERY")
    pending.cell(24, 10, "SKU Alpha")
    pending.cell(25, 2, "Pending Area")
    pending.cell(25, 5, "Reg 8")
    pending.cell(25, 7, 15000)
    pending.cell(25, 9, "Pending Store")
    pending.cell(25, 10, 2)

    workbook.create_sheet("Opening Inventory")
    opening = workbook["Opening Inventory"]
    opening.cell(5, 9, "Expected in G2G For Loading")
    opening.cell(5, 10, 12)
    opening.cell(5, 11, 4)
    opening.cell(7, 9, "Remaining Inventory After Loading")
    opening.cell(7, 10, 10)
    opening.cell(7, 11, 4)
    opening.cell(2, 10, "SKU Alpha")
    opening.cell(2, 11, "SKU Beta")

    populate_day_sheet("Mon", "Store One", 2, 1)
    populate_day_sheet("Tues", "Store Two", 3, 0.5)
    populate_load_list("LL Mon", 3, 1)
    populate_load_list("LL Tue", 4, 0.5)

    assumptions = workbook.create_sheet("Assumptions")
    assumptions.append(
        ["Company", "SKU", "Units", "Pack Weight (kg)", "Value", "Min Inventory", "Retail Store", "Region"]
    )
    assumptions.append(["Comp A", "SKU Alpha", 12, 4, 1000, 2, "Store One", "Reg 1"])
    assumptions.append(["Comp B", "SKU Beta", 12, 3, 900, 1, "Store Two", "Reg 2"])

    fees = workbook.create_sheet("BP NEW FEES")
    fees.append(
        [
            "Brand Partner",
            "SKU",
            "Units",
            "Packaging",
            "Vatable (Yes/No)",
            "Retail Value Without VAT",
            "VAT",
            "Retail Value",
            "Retail Deliveries Value",
            "Percentage Retail Deliveries",
            "Payment Collection Value",
            "Percentage Payment Collection",
        ]
    )
    fees.append(["Comp A", "SKU Alpha", 12, "Cartons", "Yes", 900, 100, 1000, 120, 0.1, 80, 0.05])
    fees.append(["Comp B", "SKU Beta", 12, "Cartons", "No", 700, 0, 700, 90, 0.1, 50, 0.05])

    notes = workbook.create_sheet("NOTES FOR USER")
    notes.append(["Line 1"])
    notes.append(["Line 2"])

    stream = BytesIO()
    workbook.save(stream)
    stream.seek(0)
    return stream


def build_loading_tracker_uom_workbook() -> BytesIO:
    workbook = Workbook()
    ws = workbook.active
    ws.title = "UOM"
    ws.append(["ITEM", "UOM", "ALT UOM", "Conversion", "Vatable", "Prices"])
    ws.append(["SKU Alpha", "ctn", "unt", 12, "No", 100])
    ws.append(["SKU Beta", "ctn", "unt", 12, "No", 80])
    stream = BytesIO()
    workbook.save(stream)
    stream.seek(0)
    return stream


def test_end_to_end_review_and_alias_memory() -> None:
    with TemporaryDirectory() as temp_dir:
        app = create_app(
            {
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{Path(temp_dir) / 'test.db'}",
                "APP_TIMEZONE": "Africa/Lagos",
            }
        )

        client = app.test_client()
        workbook = build_test_workbook()

        response = client.post(
            "/uom/import",
            data={"uom_workbook": (BytesIO(workbook.getvalue()), "uom.xlsx")},
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        assert response.status_code == 200
        assert "UOM import complete" in response.get_data(as_text=True)

        response = client.post(
            "/runs/import",
            data={"tracker_workbook": (BytesIO(workbook.getvalue()), "tracker.xlsx")},
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        assert response.status_code == 302
        assert "/review" in response.headers["Location"]

        run_id = response.headers["Location"].split("/runs/")[1].split("/")[0]

        with app.app_context():
            run = db.session.get(UploadRun, run_id)
            vanilla = db.session.query(Product).filter_by(sku_name="SKU Vanilla").one()
            assert run is not None
            assert run.rows_detected == 3
            assert run.rows_needing_review == 1

        response = client.post(
            f"/runs/{run_id}/review",
            data={"resolution::SKU Vanila": str(vanilla.id)},
            follow_redirects=True,
        )
        assert response.status_code == 200
        html = response.get_data(as_text=True)
        assert "rows are now ready for export" in html
        assert "Download XLS" in html

        download = client.get(f"/runs/{run_id}/download")
        assert download.status_code == 200
        assert download.mimetype == "application/vnd.ms-excel"
        disposition = download.headers["Content-Disposition"]
        assert "DALA Delivery Note -" in disposition

        with app.app_context():
            alias = db.session.query(ProductAlias).filter_by(alias_name="SKU Vanila").one_or_none()
            run = db.session.get(UploadRun, run_id)
            assert alias is not None
            assert run is not None
            assert run.status == "exported"
            assert run.invoice_date in disposition
            assert "tracker" in disposition
            db.session.remove()
            db.engine.dispose()


def test_database_url_uses_psycopg_driver(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgres://user:pass@db.example.com:5432/delivery")
    assert _database_uri("unused") == "postgresql+psycopg://user:pass@db.example.com:5432/delivery"

    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@db.example.com:5432/delivery")
    assert _database_uri("unused") == "postgresql+psycopg://user:pass@db.example.com:5432/delivery"


def test_uom_import_sets_active_source_of_truth() -> None:
    with TemporaryDirectory() as temp_dir:
        app = create_app(
            {
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{Path(temp_dir) / 'test.db'}",
                "APP_TIMEZONE": "Africa/Lagos",
            }
        )

        client = app.test_client()

        first = Workbook()
        uom_one = first.active
        uom_one.title = "UOM"
        uom_one.append(["ITEM", "UOM", "ALT UOM", "Conversion", "Vatable", "Prices"])
        uom_one.append(["SKU Alpha", "ctn", "unt", 12, "Yes", 100])
        uom_one.append(["SKU Vanilla", "ctn", "unt", 12, "No", 80])
        first_bytes = BytesIO()
        first.save(first_bytes)
        first_bytes.seek(0)

        second = Workbook()
        uom_two = second.active
        uom_two.title = "UOM"
        uom_two.append(["ITEM", "UOM", "ALT UOM", "Conversion", "Vatable", "Prices"])
        uom_two.append(["SKU Alpha", "ctn", "unt", 12, "Yes", 120])
        second_bytes = BytesIO()
        second.save(second_bytes)
        second_bytes.seek(0)

        response = client.post(
            "/uom/import",
            data={"uom_workbook": (BytesIO(first_bytes.getvalue()), "uom-one.xlsx")},
            content_type="multipart/form-data",
        )
        assert response.status_code == 302

        response = client.post(
            "/uom/import",
            data={"uom_workbook": (BytesIO(second_bytes.getvalue()), "uom-two.xlsx")},
            content_type="multipart/form-data",
        )
        assert response.status_code == 302

        with app.app_context():
            alpha = db.session.query(Product).filter_by(sku_name="SKU Alpha").one()
            vanilla = db.session.query(Product).filter_by(sku_name="SKU Vanilla").one()
            assert alpha.is_active is True
            assert float(alpha.price) == 120.0
            assert vanilla.is_active is False
            db.session.remove()
            db.engine.dispose()


def test_product_master_can_add_edit_deactivate_and_restore() -> None:
    with TemporaryDirectory() as temp_dir:
        app = create_app(
            {
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{Path(temp_dir) / 'test.db'}",
                "APP_TIMEZONE": "Africa/Lagos",
            }
        )

        client = app.test_client()

        response = client.post(
            "/products",
            data={
                "sku_name": "Manual Product",
                "price": "24500",
                "vatable": "yes",
                "uom": "ctn",
                "alt_uom": "pcs",
                "conversion": "12",
            },
            follow_redirects=True,
        )
        assert response.status_code == 200
        assert "was added to the product master" in response.get_data(as_text=True)

        with app.app_context():
            product = db.session.query(Product).filter_by(sku_name="Manual Product").one()
            product_id = product.id

        response = client.post(
            f"/products/{product_id}/edit",
            data={
                "sku_name": "Manual Product Updated",
                "price": "26000",
                "vatable": "no",
                "uom": "ctn",
                "alt_uom": "pcs",
                "conversion": "6",
            },
            follow_redirects=True,
        )
        assert response.status_code == 200
        assert "was updated" in response.get_data(as_text=True)

        response = client.post(f"/products/{product_id}/deactivate", follow_redirects=True)
        assert response.status_code == 200
        assert "removed from the active product master" in response.get_data(as_text=True)

        response = client.post(f"/products/{product_id}/activate", follow_redirects=True)
        assert response.status_code == 200
        assert "is active again" in response.get_data(as_text=True)

        with app.app_context():
            product = db.session.get(Product, product_id)
            assert product is not None
            assert product.sku_name == "Manual Product Updated"
            assert float(product.price) == 26000.0
            assert product.vatable is False
            assert product.is_active is True
            db.session.remove()
            db.engine.dispose()


def test_manual_product_stays_active_after_new_uom_import() -> None:
    with TemporaryDirectory() as temp_dir:
        app = create_app(
            {
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{Path(temp_dir) / 'test.db'}",
                "APP_TIMEZONE": "Africa/Lagos",
            }
        )

        client = app.test_client()

        workbook = build_test_workbook()
        response = client.post(
            "/uom/import",
            data={"uom_workbook": (BytesIO(workbook.getvalue()), "uom.xlsx")},
            content_type="multipart/form-data",
        )
        assert response.status_code == 302

        response = client.post(
            "/products",
            data={
                "sku_name": "Manual Product",
                "price": "24500",
                "vatable": "yes",
                "uom": "ctn",
                "alt_uom": "pcs",
                "conversion": "12",
            },
        )
        assert response.status_code == 302

        second = Workbook()
        uom_two = second.active
        uom_two.title = "UOM"
        uom_two.append(["ITEM", "UOM", "ALT UOM", "Conversion", "Vatable", "Prices"])
        uom_two.append(["SKU Alpha", "ctn", "unt", 12, "Yes", 120])
        second_bytes = BytesIO()
        second.save(second_bytes)
        second_bytes.seek(0)

        response = client.post(
            "/uom/import",
            data={"uom_workbook": (BytesIO(second_bytes.getvalue()), "uom-two.xlsx")},
            content_type="multipart/form-data",
        )
        assert response.status_code == 302

        with app.app_context():
            alpha = db.session.query(Product).filter_by(sku_name="SKU Alpha").one()
            vanilla = db.session.query(Product).filter_by(sku_name="SKU Vanilla").one()
            manual = db.session.query(Product).filter_by(sku_name="Manual Product").one()
            assert alpha.is_active is True
            assert vanilla.is_active is False
            assert manual.is_active is True
            db.session.remove()
            db.engine.dispose()


def test_seed_bootstrap_populates_database_when_empty() -> None:
    with TemporaryDirectory() as temp_dir:
        app = create_app(
            {
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{Path(temp_dir) / 'test.db'}",
                "APP_TIMEZONE": "Africa/Lagos",
            }
        )

        with app.app_context():
            seeded = bootstrap_seed_uom_if_empty()
            assert seeded is not None
            assert seeded.product_count > 0
            assert db.session.query(Product).count() == seeded.product_count
            db.session.remove()
            db.engine.dispose()


def test_tracker_import_accepts_mon_test_sheet_name() -> None:
    with TemporaryDirectory() as temp_dir:
        app = create_app(
            {
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{Path(temp_dir) / 'test.db'}",
                "APP_TIMEZONE": "Africa/Lagos",
            }
        )

        client = app.test_client()
        workbook = build_test_workbook("Mon_Test")

        response = client.post(
            "/uom/import",
            data={"uom_workbook": (BytesIO(workbook.getvalue()), "uom.xlsx")},
            content_type="multipart/form-data",
        )
        assert response.status_code == 302

        response = client.post(
            "/runs/import",
            data={"tracker_workbook": (BytesIO(workbook.getvalue()), "LT-DN Test 2.xlsx")},
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        assert response.status_code == 302
        assert "/review" in response.headers["Location"]

        with app.app_context():
            db.session.remove()
            db.engine.dispose()


def test_item_list_export_can_merge_into_uom_without_replacing_existing() -> None:
    with TemporaryDirectory() as temp_dir:
        app = create_app(
            {
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{Path(temp_dir) / 'test.db'}",
                "APP_TIMEZONE": "Africa/Lagos",
            }
        )

        client = app.test_client()
        workbook = build_test_workbook()

        response = client.post(
            "/uom/import",
            data={"uom_workbook": (BytesIO(workbook.getvalue()), "uom.xlsx")},
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        assert response.status_code == 200

        response = client.post(
            "/products",
            data={
                "sku_name": "Existing SKU Alpha",
                "price": "1200",
                "vatable": "yes",
                "uom": "ctn",
                "alt_uom": "pcs",
                "conversion": "12",
            },
            follow_redirects=True,
        )
        assert response.status_code == 200

        item_list = build_item_list_export()
        response = client.post(
            "/uom/import",
            data={"uom_workbook": (BytesIO(item_list.getvalue()), "item_list (5).xls")},
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert "1 new product rows were added and 1 existing items were skipped and 1 items were moved to inactive" in html

        with app.app_context():
            existing = db.session.query(Product).filter_by(sku_name="Existing SKU Alpha").one()
            fresh = db.session.query(Product).filter_by(sku_name="Fresh SKU Beta").one()
            retired = db.session.query(Product).filter_by(sku_name="Retired SKU Gamma").one()
            assert float(existing.price) == 1200.0
            assert fresh.is_active is True
            assert float(fresh.price) == 3450.0
            assert fresh.uom == "ctn"
            assert fresh.alt_uom == "unt"
            assert fresh.vatable is False
            assert retired.is_active is False
            db.session.remove()
            db.engine.dispose()


def test_tracker_ignores_known_inactive_products() -> None:
    with TemporaryDirectory() as temp_dir:
        app = create_app(
            {
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{Path(temp_dir) / 'test.db'}",
                "APP_TIMEZONE": "Africa/Lagos",
            }
        )

        client = app.test_client()
        workbook = build_test_workbook()

        response = client.post(
            "/uom/import",
            data={"uom_workbook": (BytesIO(workbook.getvalue()), "uom.xlsx")},
            content_type="multipart/form-data",
        )
        assert response.status_code == 302

        response = client.post(
            "/products",
            data={
                "sku_name": "Mamaologi SKU",
                "price": "1",
                "vatable": "no",
                "uom": "ctn",
                "alt_uom": "pcs",
                "conversion": "10",
            },
        )
        assert response.status_code == 302

        with app.app_context():
            product = db.session.query(Product).filter_by(sku_name="Mamaologi SKU").one()
            product_id = product.id

        response = client.post(f"/products/{product_id}/deactivate", follow_redirects=True)
        assert response.status_code == 200

        tracker = Workbook()
        sheet = tracker.active
        sheet.title = "Sheet1"
        sheet.append(["Sales Order Number", "Stores", "Mamaologi SKU"])
        sheet.append(["SO-200", "Market One", 3])
        tracker_bytes = BytesIO()
        tracker.save(tracker_bytes)
        tracker_bytes.seek(0)

        response = client.post(
            "/runs/import",
            data={"tracker_workbook": (BytesIO(tracker_bytes.getvalue()), "mamaologi.xlsx")},
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert "Everything active is matched and ready." in html
        assert "Ignored" in html

        with app.app_context():
            db.session.remove()
            db.engine.dispose()


def test_review_can_mark_source_sku_inactive_directly() -> None:
    with TemporaryDirectory() as temp_dir:
        app = create_app(
            {
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{Path(temp_dir) / 'test.db'}",
                "APP_TIMEZONE": "Africa/Lagos",
            }
        )

        client = app.test_client()
        workbook = build_test_workbook()

        response = client.post(
            "/uom/import",
            data={"uom_workbook": (BytesIO(workbook.getvalue()), "uom.xlsx")},
            content_type="multipart/form-data",
        )
        assert response.status_code == 302

        response = client.post(
            "/runs/import",
            data={"tracker_workbook": (BytesIO(workbook.getvalue()), "tracker.xlsx")},
            content_type="multipart/form-data",
            follow_redirects=False,
        )
        assert response.status_code == 302
        run_id = response.headers["Location"].split("/runs/")[1].split("/")[0]

        response = client.post(
            f"/runs/{run_id}/review",
            data={"mark_inactive": "SKU Vanila"},
            follow_redirects=True,
        )
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert "was moved to inactive" in html
        assert "Inactive items skipped" in html
        assert "SKU Vanila" in html

        with app.app_context():
            inactive_product = db.session.query(Product).filter_by(sku_name="SKU Vanila").one()
            run = db.session.get(UploadRun, run_id)
            assert inactive_product.is_active is False
            assert run is not None
            assert run.rows_needing_review == 0
            db.session.remove()
            db.engine.dispose()


def test_general_dashboard_and_delivery_note_module_render() -> None:
    with TemporaryDirectory() as temp_dir:
        app = create_app(
            {
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{Path(temp_dir) / 'test.db'}",
                "APP_TIMEZONE": "Africa/Lagos",
            }
        )

        client = app.test_client()

        response = client.get("/")
        assert response.status_code == 200
        html = response.get_data(as_text=True)
        assert "Planning, loading, exports, and future automators in one calm workspace." in html
        assert "Delivery Note" in html
        assert "Loading Tracker" in html
        assert "SKU Automator" in html

        response = client.get("/delivery-note")
        assert response.status_code == 200
        assert "Delivery Note studio" in response.get_data(as_text=True)

        with app.app_context():
            db.session.remove()
            db.engine.dispose()


def test_loading_tracker_import_builds_initial_module_views() -> None:
    with TemporaryDirectory() as temp_dir:
        app = create_app(
            {
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{Path(temp_dir) / 'test.db'}",
                "APP_TIMEZONE": "Africa/Lagos",
            }
        )

        client = app.test_client()
        workbook = build_loading_tracker_workbook()

        response = client.post(
            "/loading-tracker/import",
            data={"loading_tracker_workbook": (BytesIO(workbook.getvalue()), "Week 4 Loading Tracker.xlsx")},
            content_type="multipart/form-data",
            follow_redirects=True,
        )
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert "Loading tracker imported." in html
        assert "Week 4 Loading Tracker" in html

        with app.app_context():
            tracker_import = db.session.query(LoadingTrackerImport).one()
            mon = db.session.query(LoadingTrackerDay).filter_by(day_name="Mon").one()
            assert tracker_import.assumptions_sku_count == 2
            assert len(tracker_import.pending_rows_json) == 1
            assert len(tracker_import.planning_rows) == 5
            assert len(tracker_import.inventory_items) == 2
            assert mon.batch_count == 2
            assert mon.active_store_count == 2
            assert float(mon.load_total) == 4.0
            import_id = tracker_import.id

        day_response = client.get(f"/loading-tracker/imports/{import_id}/days/Mon")
        assert day_response.status_code == 200
        day_html = day_response.get_data(as_text=True)
        assert "Mon is now editable inside the planner." in day_html
        assert "Generated LL" in day_html
        assert "Store One" in day_html

        pending_response = client.get(f"/loading-tracker/imports/{import_id}/pending")
        assert pending_response.status_code == 200
        pending_html = pending_response.get_data(as_text=True)
        assert "Pending is now a live queue" in pending_html
        assert "Pending Store" in pending_html

        with app.app_context():
            db.session.remove()
            db.engine.dispose()


def test_loading_tracker_live_move_and_inventory_adjustment() -> None:
    with TemporaryDirectory() as temp_dir:
        app = create_app(
            {
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{Path(temp_dir) / 'test.db'}",
                "APP_TIMEZONE": "Africa/Lagos",
            }
        )

        client = app.test_client()
        workbook = build_loading_tracker_workbook()

        response = client.post(
            "/loading-tracker/import",
            data={"loading_tracker_workbook": (BytesIO(workbook.getvalue()), "Week 4 Loading Tracker.xlsx")},
            content_type="multipart/form-data",
        )
        assert response.status_code == 302

        with app.app_context():
            tracker_import = db.session.query(LoadingTrackerImport).one()
            pending_row = db.session.query(LoadingTrackerRow).filter_by(row_state="pending").one()
            import_id = tracker_import.id
            pending_row_id = pending_row.id

        response = client.post(
            f"/loading-tracker/imports/{import_id}/rows/{pending_row_id}/move",
            data={"target_day_name": "Tues"},
            follow_redirects=True,
        )
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert "moved into Tues" in html
        assert "Pending Store" in html

        response = client.post(
            f"/loading-tracker/imports/{import_id}/inventory",
            data={"sku_name": "SKU Alpha", "added_qty": "3", "opening_g2g_qty": "12", "opening_remaining_qty": "10"},
            follow_redirects=True,
        )
        html = response.get_data(as_text=True)
        assert response.status_code == 200
        assert "Inventory for" in html and "was updated" in html

        with app.app_context():
            pending_count = db.session.query(LoadingTrackerRow).filter_by(row_state="pending").count()
            tracker_import = db.session.query(LoadingTrackerImport).one()
            sku_alpha = next(item for item in tracker_import.inventory_items if item.sku_name == "SKU Alpha")
            assert pending_count == 0
            assert float(sku_alpha.added_qty) == 3.0
            db.session.remove()
            db.engine.dispose()


def test_loading_tracker_counts_handoff_history_and_carry_forward() -> None:
    with TemporaryDirectory() as temp_dir:
        app = create_app(
            {
                "TESTING": True,
                "SQLALCHEMY_DATABASE_URI": f"sqlite:///{Path(temp_dir) / 'test.db'}",
                "APP_TIMEZONE": "Africa/Lagos",
            }
        )

        client = app.test_client()

        response = client.post(
            "/uom/import",
            data={"uom_workbook": (BytesIO(build_loading_tracker_uom_workbook().getvalue()), "uom.xlsx")},
            content_type="multipart/form-data",
        )
        assert response.status_code == 302

        response = client.post(
            "/loading-tracker/import",
            data={"loading_tracker_workbook": (BytesIO(build_loading_tracker_workbook().getvalue()), "Week 4 Loading Tracker.xlsx")},
            content_type="multipart/form-data",
        )
        assert response.status_code == 302

        with app.app_context():
            tracker_import = db.session.query(LoadingTrackerImport).one()
            mon = db.session.query(LoadingTrackerDay).filter_by(day_name="Mon").one()
            mon_first_row = (
                db.session.query(LoadingTrackerRow)
                .filter_by(day_id=mon.id, row_state="planned")
                .order_by(LoadingTrackerRow.sort_order.asc())
                .first()
            )
            assert mon_first_row is not None
            import_id = tracker_import.id
            row_id = mon_first_row.id

        counts = client.post(
            f"/loading-tracker/imports/{import_id}/days/Mon/counts",
            data={"count::SKU Alpha": "9", "count::SKU Beta": "3.5"},
            follow_redirects=True,
        )
        counts_html = counts.get_data(as_text=True)
        assert counts.status_code == 200
        assert "Start-of-day physical count saved for Mon" in counts_html
        assert "Count status" in counts_html

        with app.app_context():
            assert db.session.query(LoadingTrackerDailyCount).filter_by(day_id=mon.id).count() == 2

        invalid_pending = client.post(
            f"/loading-tracker/imports/{import_id}/rows/{row_id}/move",
            data={"target_day_name": "__pending__"},
            follow_redirects=True,
        )
        invalid_html = invalid_pending.get_data(as_text=True)
        assert invalid_pending.status_code == 200
        assert "Choose a pending reason" in invalid_html

        valid_pending = client.post(
            f"/loading-tracker/imports/{import_id}/rows/{row_id}/move",
            data={
                "target_day_name": "__pending__",
                "reason_code": "stock_shortage",
                "reason_note": "SKU Alpha short",
            },
            follow_redirects=True,
        )
        valid_pending_html = valid_pending.get_data(as_text=True)
        assert valid_pending.status_code == 200
        assert "now waiting in Pending" in valid_pending_html
        assert "Insufficient stock: SKU Alpha short" in valid_pending_html

        tues_day = client.get(f"/loading-tracker/imports/{import_id}/days/Tues")
        tues_html = tues_day.get_data(as_text=True)
        assert tues_day.status_code == 200
        assert "Pack Breaker" in tues_html
        assert "SKU Beta" in tues_html

        handoff = client.post(
            f"/loading-tracker/imports/{import_id}/days/Tues/handoff",
            follow_redirects=True,
        )
        handoff_html = handoff.get_data(as_text=True)
        assert handoff.status_code == 200
        assert "final adjusted day plan has been handed off" in handoff_html
        assert "Download XLS" in handoff_html

        history = client.get(f"/loading-tracker/imports/{import_id}/history")
        history_html = history.get_data(as_text=True)
        assert history.status_code == 200
        assert "Track planning changes" in history_html
        assert "Insufficient stock" in history_html

        history_download = client.get(f"/loading-tracker/imports/{import_id}/history/download")
        assert history_download.status_code == 200
        assert history_download.mimetype == "text/csv"
        assert b"moved_to_pending" in history_download.data

        carry = client.post(
            f"/loading-tracker/imports/{import_id}/carry-forward",
            follow_redirects=True,
        )
        carry_html = carry.get_data(as_text=True)
        assert carry.status_code == 200
        assert "fresh week was created" in carry_html

        with app.app_context():
            imports = db.session.query(LoadingTrackerImport).order_by(LoadingTrackerImport.created_at.asc()).all()
            assert len(imports) == 2
            latest_import = imports[-1]
            assert len(latest_import.days) == 6
            assert db.session.query(LoadingTrackerRow).filter_by(tracker_import_id=latest_import.id, row_state="pending").count() == 2
            assert float(latest_import.opening_g2g_total) == 7.0
            assert db.session.query(LoadingTrackerEvent).filter_by(tracker_import_id=latest_import.id).count() >= 1
            assert db.session.query(UploadRun).count() == 1
            db.session.remove()
            db.engine.dispose()
