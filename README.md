# Delivery Note Automations

This is the full delivery-note system for Railway.

## What the app does

- Imports the latest `UOM` workbook into a database-backed product master.
- Stores approved SKU corrections so repeated mistakes are fixed automatically next time.
- Uploads a loading tracker workbook and reads only lines with quantity above `0.00`.
- Builds a review step for new mismatches.
- Exports one clean delivery-note `.xls` file when every line is safe.

## Main flow

1. Import the latest UOM workbook.
2. Upload the loading tracker workbook.
3. Review unresolved SKU names only if needed.
4. Download the final `.xls` file.

## Local run

```bash
python -m pip install -r requirements.txt
python app.py
```

Open `http://localhost:8080`.

## Database

- Local default: SQLite in `instance/delivery_note.db`
- Railway production: set `DATABASE_URL` to Railway Postgres

## Railway notes

- App timezone comes from `APP_TIMEZONE` and defaults to `Africa/Lagos`
- Invoice date uses tomorrow's date in that timezone
- `Procfile` is included

## Suggested Railway environment variables

- `APP_TIMEZONE=Africa/Lagos`
- `SECRET_KEY=your-secret`
- `DATABASE_URL=<Railway Postgres URL>`
