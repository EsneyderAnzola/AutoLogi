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

    # 4. Obtener la lista de TODAS las tablas públicas de Supabase
    try:
        with engine.connect() as conn:
            query_tablas = text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            result = conn.execute(query_tablas)
            tablas = [row[0] for row in result]
        
        print(f"📂 Tablas detectadas para sincronizar: {tablas}")
    except Exception as e:
        print(f"❌ Error al listar las tablas: {e}")
        return

    # 5. Sincronizar cada tabla detectada
    for t in tablas:
        try:
            print(f"🔄 Sincronizando tabla: {t}...")
            with engine.connect() as conn:
                df = pd.read_sql(text(f'SELECT * FROM "{t}"'), conn)
            
            # Procesamiento especial para la hoja "Ingreso"
            if t.lower() == 'ingreso':
                # Convertir formato de fecha si existe la columna "Fecha"
                if 'Fecha' in df.columns:
                    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Aplicar estructura específica con el orden correcto
                columnas_estructura = [
                    'telefono', 'Fecha', 'nombre', 'cedula', 'ciudad', 
                    'grupo', 'pdv', 'latitud', 'Longitud', 'foto', 
                    'Cierre', 'Seccion'
                ]
                
                # Seleccionar solo las columnas que existen en el DataFrame
                columnas_existentes = [col for col in columnas_estructura if col in df.columns]
                df = df[columnas_existentes]
            
            # Buscar la hoja por nombre; si no existe, la crea
            try:
                worksheet = spreadsheet.worksheet(t)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=t, rows="100", cols="20")
            
            # Limpiar la hoja y pegar los nuevos datos
            worksheet.clear()
            set_with_dataframe(worksheet, df)
            print(f"✅ Sincronizada con éxito: {t} ({len(df)} registros)")
            
        except Exception as e:
            print(f"❌ Error al procesar la tabla {t}: {e}")

if __name__ == "__main__":
    sync()
