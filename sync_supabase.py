import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

def sync():
    # 1. Configurar conexiones desde los Secrets de GitHub
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

    try:
        print(f"🔄 Sincronizando tabla: Ingreso...")
        
        with engine.connect() as conn:
            # Traer solo datos desde el 30 de enero de 2026
            query = text('''
                SELECT telefono, nombre, cedula, ciudad, grupo, pdv, latitud, "Longitud", foto, "Cierre", "Seccion"
                FROM "Ingreso" 
                WHERE created >= '2026-01-30 00:00:00'
                ORDER BY created DESC
            ''')
            df = pd.read_sql(query, conn)
        
        # Estructura final con el orden exacto de la segunda imagen
        df_final = df[[
            'telefono',    # Columna A
            'nombre',      # Columna B
            'cedula',      # Columna C
            'ciudad',      # Columna D
            'grupo',       # Columna E
            'pdv',         # Columna F
            'latitud',     # Columna G
            'Longitud',    # Columna H
            'foto',        # Columna I
            'Cierre',      # Columna J
            'Seccion'      # Columna K
        ]]
        
        # Buscar la hoja "Ingreso"; si no existe, la crea
        try:
            worksheet = spreadsheet.worksheet('Ingreso')
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title='Ingreso', rows="1000", cols="11")
        
        # ELIMINAR datos previos y pegar los nuevos datos
        worksheet.clear()
        set_with_dataframe(worksheet, df_final)
        print(f"✅ Sincronizada con éxito: Ingreso ({len(df_final)} registros desde 2026-01-30)")
        
    except Exception as e:
        print(f"❌ Error al procesar la tabla Ingreso: {e}")

if __name__ == "__main__":
    sync()


