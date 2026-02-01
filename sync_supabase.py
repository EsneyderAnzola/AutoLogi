import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

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
            
            # Obtener fecha actual (solo la parte de fecha, sin hora)
            fecha_actual = datetime.now().strftime('%Y-%m-%d')
            
            with engine.connect() as conn:
                # SELECT solo los datos del día actual (filtrando por la columna "Fecha")
                # Ajusta el nombre de la columna si es diferente en tu tabla
                query = text(f'''
                    SELECT * FROM "{t}" 
                    WHERE DATE("Fecha") = :fecha_actual
                ''')
                df_hoy = pd.read_sql(query, conn, params={'fecha_actual': fecha_actual})
            
            # Convertir formato de fecha si existe la columna "Fecha"
            if 'Fecha' in df_hoy.columns:
                df_hoy['Fecha'] = pd.to_datetime(df_hoy['Fecha']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Buscar la hoja por nombre; si no existe, la crea
            try:
                worksheet = spreadsheet.worksheet(t)
                # Leer datos existentes en el sheet
                df_existente = get_as_dataframe(worksheet, evaluate_formulas=True)
                # Limpiar filas completamente vacías
                df_existente = df_existente.dropna(how='all')
                
                # Filtrar datos existentes: mantener solo los que NO son del día actual
                if 'Fecha' in df_existente.columns and len(df_existente) > 0:
                    df_existente['Fecha'] = pd.to_datetime(df_existente['Fecha'], errors='coerce')
                    df_previos = df_existente[df_existente['Fecha'].dt.date != datetime.strptime(fecha_actual, '%Y-%m-%d').date()]
                    df_previos['Fecha'] = df_previos['Fecha'].dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    df_previos = pd.DataFrame()
                    
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=t, rows="100", cols="20")
                df_previos = pd.DataFrame()
            
            # Combinar datos previos + datos de hoy
            if len(df_previos) > 0:
                df_final = pd.concat([df_previos, df_hoy], ignore_index=True)
            else:
                df_final = df_hoy
            
            # Limpiar la hoja y pegar los datos actualizados
            worksheet.clear()
            set_with_dataframe(worksheet, df_final)
            print(f"✅ Sincronizada con éxito: {t} ({len(df_hoy)} registros nuevos de hoy, {len(df_previos)} registros previos mantenidos)")
            
        except Exception as e:
            print(f"❌ Error al procesar la tabla {t}: {e}")

if __name__ == "__main__":
    sync()
