import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

def sync():
    # 1. Configurar conexiones desde los Secrets
    db_url = os.environ['DB_URL']
    sheet_id = '1MhtXjziojWiYjLRbYkmfrd-Dt2vsie102n6_CUFteFg'
    
    # 2. Conectar a Supabase
    engine = create_engine(db_url)
    
    # 3. Conectar a Google Sheets
    creds_dict = json.loads(os.environ['GOOGLE_CREDENTIALS'])
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)

    # Lista de tablas que quieres llevar al Sheets
    tablas = ["Ingreso", "Inventario"]

    for t in tablas:
        try:
            print(f"🔄 Procesando tabla: {t}...")
            with engine.connect() as conn:
                # Usamos comillas dobles por si el nombre tiene mayúsculas
                query = text(f'SELECT * FROM "{t}"')
                df = pd.read_sql(query, conn)
            
            # Buscar la hoja o crearla si no existe
            try:
                worksheet = spreadsheet.worksheet(t)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=t, rows="100", cols="20")
            
            # Limpiar la hoja y pegar los datos nuevos
            worksheet.clear()
            set_with_dataframe(worksheet, df)
            print(f"✅ Sincronizada con éxito: {t}")
            
        except Exception as e:
            print(f"❌ Error en tabla {t}: {e}")

if __name__ == "__main__":
    sync()
