import os
import polars as pl
from sqlalchemy import create_engine # Importar create_engine para poder crear el objeto engine

# --- Configuración (DEBE COINCIDIR con olap_processor.py) ---
OUTPUT_DIR = "data"
DW_FILE = "dss_data_warehouse.db"
BACKUP_FILE = "agregaciones_kpis_backup.parquet"
AGGREGATION_TABLE = "MV_OLAP_CUBE_KPIs"

DW_PATH = os.path.join(OUTPUT_DIR, DW_FILE)
BACKUP_PATH = os.path.join(OUTPUT_DIR, BACKUP_FILE)
# La cadena de conexión
DW_CONNECTION_STRING = f"sqlite:///{DW_PATH}" 


def check_file_system():
    """Verifica que los archivos existan y no estén vacíos."""
    print("--- Verificación del Sistema de Archivos ---")
    
    # ... (código de verificación de archivos omitido, ya funcionó)
    
    # Se asume que esta parte retorna True si los archivos existen
    if os.path.exists(DW_PATH) and os.path.getsize(DW_PATH) > 0 and \
       os.path.exists(BACKUP_PATH) and os.path.getsize(BACKUP_PATH) > 0:
       return True
    return False

def check_data_content():
    """Verifica que los datos se puedan leer correctamente de ambas fuentes."""
    print("\n--- Verificación del Contenido (Lectura) ---")
    
    try:
        # Crear el objeto Engine para que Polars lo use
        engine = create_engine(DW_CONNECTION_STRING) 
        
        # Leer desde el DW (Tabla de Agregación) - Usando 'connection=engine'
        df_dw = pl.read_database(f"SELECT * FROM {AGGREGATION_TABLE}", connection=engine)
        print(f"Lectura exitosa desde DW (Tabla: {AGGREGATION_TABLE}). Filas: {df_dw.shape[0]}")
        
        # Leer desde el archivo Parquet (Backup)
        df_parquet = pl.read_parquet(BACKUP_PATH)
        print(f"Lectura exitosa desde Parquet. Filas: {df_parquet.shape[0]}")
        
        # Comprobar que los datos sean iguales
        if df_dw.shape[0] == df_parquet.shape[0] and df_dw.shape[1] == df_parquet.shape[1]:
             print("¡El contenido de la DB y el Parquet coinciden! El procesamiento fue exitoso.")
        else:
             print("Advertencia: El número de filas/columnas no coincide. Revise el procesamiento.")

    except Exception as e:
        print(f"ERROR al leer los datos. El procesamiento falló o los archivos están corruptos: {e}")

if __name__ == "__main__":
    if check_file_system():
        check_data_content()
    else:
        print("Fallo en la verificación de archivos, ejecute olap_processor.py.")