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

## Tally Bridge agent for a remote Tally server

If the main app is hosted on Railway, it cannot write directly into `C:\...` on the Tally server. In that setup, run the local bridge agent on the Tally machine:

```bash
python scripts/tally_bridge_agent.py --base-dir C:\TallyBridge --port 9000
```

This creates:

- `C:\TallyBridge\inbox` for incoming Sales Order payloads from Railway
- `C:\TallyBridge\outbox` for returned Tally register files
- `C:\TallyBridge\archive` for claimed register copies

Use the Tally Bridge profile like this:

- `Connection Mode = XML / HTTP`
- `Endpoint = http://<tally-server-ip>:9000`

How the flow works:

1. Railway sends the Sales Order payload to the local bridge agent.
2. The agent writes it into `C:\TallyBridge\inbox`.
3. Tally-side operators import from the inbox folder.
4. Tally exports the returned register into `C:\TallyBridge\outbox`.
5. Railway pulls the latest register back over HTTP and links it into SKU Automator.

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
