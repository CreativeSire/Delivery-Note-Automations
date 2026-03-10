from __future__ import annotations

from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

from openpyxl import Workbook

from app import _database_uri, create_app
from models import Product, ProductAlias, UploadRun, db
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
    )
    return BytesIO(payload.encode("utf-8"))


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

        with app.app_context():
            alias = db.session.query(ProductAlias).filter_by(alias_name="SKU Vanila").one_or_none()
            run = db.session.get(UploadRun, run_id)
            assert alias is not None
            assert run is not None
            assert run.status == "exported"
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
        assert "1 new product rows were added and 1 existing items were skipped" in html

        with app.app_context():
            existing = db.session.query(Product).filter_by(sku_name="Existing SKU Alpha").one()
            fresh = db.session.query(Product).filter_by(sku_name="Fresh SKU Beta").one()
            assert float(existing.price) == 1200.0
            assert fresh.is_active is True
            assert float(fresh.price) == 3450.0
            assert fresh.uom == "ctn"
            assert fresh.alt_uom == "unt"
            assert fresh.vatable is False
            db.session.remove()
            db.engine.dispose()
