import os
import sys
import json
import datetime as dt
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor

import psycopg
import gspread
from google.oauth2.service_account import Credentials

SHEET_ID = '1MhtXjziojWiYjLRbYkmfrd-Dt2vsie102n6_CUFteFg'


def norm(v):
    """Convierte un valor de Postgres a algo que Google Sheets acepte."""
    if v is None:
        return ""
    if isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, dt.datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, (dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def read_table(db_url, t):
    with psycopg.connect(db_url) as conn, conn.cursor() as cur:
        ident = t.replace('"', '""')
        cur.execute(f'SELECT * FROM "{ident}"')
        header = [c.name for c in cur.description]
        rows = [[norm(v) for v in r] for r in cur.fetchall()]
    return t, [header] + rows


def open_sheet(creds_json):
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scopes)
    sp = gspread.authorize(creds).open_by_key(SHEET_ID)
    return sp, sp.fetch_sheet_metadata()


def q(title):
    return "'" + title.replace("'", "''") + "'"


def sync():
    db_url = os.environ['DB_URL'] \
        .replace('postgresql+psycopg2://', 'postgresql://') \
        .replace('postgresql+psycopg://', 'postgresql://')
    errors = []

    with ThreadPoolExecutor(max_workers=8) as pool:
        # Google en segundo plano mientras leemos Supabase
        gfut = pool.submit(open_sheet, os.environ['GOOGLE_CREDENTIALS'])

        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = 'public'")
            tablas = [r[0] for r in cur.fetchall()]
        print(f"📂 Tablas: {tablas}")

        futs = {t: pool.submit(read_table, db_url, t) for t in tablas}
        data = {}
        for t, f in futs.items():
            try:
                data[t] = f.result()[1]
            except Exception as e:
                errors.append(t)
                print(f"❌ Error leyendo {t}: {e}")

        sp, meta = gfut.result()

    existing = {s['properties']['title']: s['properties'] for s in meta['sheets']}

    # 1) Crear hojas que falten / agrandar las que queden chicas (1 llamada)
    reqs = []
    for t, values in data.items():
        rows, cols = max(len(values), 1), max(len(values[0]), 1)
        if t not in existing:
            reqs.append({'addSheet': {'properties': {
                'title': t,
                'gridProperties': {'rowCount': rows, 'columnCount': cols}}}})
        else:
            g = existing[t]['gridProperties']
            if g['rowCount'] < rows or g['columnCount'] < cols:
                reqs.append({'updateSheetProperties': {
                    'properties': {
                        'sheetId': existing[t]['sheetId'],
                        'gridProperties': {
                            'rowCount': max(g['rowCount'], rows),
                            'columnCount': max(g['columnCount'], cols)}},
                    'fields': 'gridProperties.rowCount,gridProperties.columnCount'}})
    if reqs:
        sp.batch_update({'requests': reqs})

    # 2) Limpiar solo las hojas que ya existian (1 llamada)
    to_clear = [q(t) for t in data if t in existing]
    if to_clear:
        sp.values_batch_clear(body={'ranges': to_clear})

    # 3) Escribir todo de una vez (1 llamada)
    if data:
        sp.values_batch_update(body={
            'valueInputOption': 'USER_ENTERED',
            'data': [{'range': f"{q(t)}!A1", 'values': v} for t, v in data.items()],
        })
    print(f"✅ Sincronizadas {len(data)} tablas")

    if errors:
        print(f"⚠️ Fallaron: {errors}")
        sys.exit(1)


if __name__ == "__main__":
    sync()
