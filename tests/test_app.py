from __future__ import annotations

from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

from openpyxl import Workbook

from app import create_app
from models import Product, ProductAlias, UploadRun, db


def build_test_workbook() -> BytesIO:
    workbook = Workbook()
    tracker = workbook.active
    tracker.title = "tracker"
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
        assert "Download final `.xls` file" in html

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
