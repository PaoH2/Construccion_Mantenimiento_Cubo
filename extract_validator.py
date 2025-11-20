import polars as pl
from sqlalchemy import create_engine, text
import os
from typing import Dict, Union, List 
# --- CONFIGURACIÓN DE PRUEBA ---
OUTPUT_DIR = "data"
DW_FILE = "dss_data_warehouse.db"
DW_PATH = os.path.join(OUTPUT_DIR, DW_FILE)
DW_CONNECTION_STRING = f"sqlite:///{DW_PATH}" 

# DEFINICIÓN DEL ESQUEMA ESPERADO
# Aquí defines las expectativas para la fuente de datos bruta (tablas de hecho y dimensión)
# Nota: Como usamos la función create_raw_dss_data(), estas son las columnas base.

# Vamos a validar la tabla de AGREGACIÓN (MV_OLAP_CUBE_KPIs)
EXPECTED_SCHEMA: Dict[str, Dict[str, Union[int, str, List[str]]]] = {
    # Tabla de Agregación Final (MV_OLAP_CUBE_KPIs)
    "agg_cube": { 
        # Valor mínimo esperado para la tabla de agregación
        # Un valor seguro es 10.
        "min_rows": 10, 
        # Ya que el procesador dice que crea 24, podemos usar 20 como mínimo seguro.
        "expected_columns": ["Region", "Anio", "Tasa_Defectos_Promedio", "Total_Defectos_Absoluto"]
    }
}

# --- FUNCIÓN DE PRUEBA CORREGIDA ---

def validate_extraction_completeness(engine) -> bool:
    """Ejecuta pruebas de validación de completitud e integridad."""
    
    print("\n===== INICIANDO VALIDACIÓN DE LA EXTRACCIÓN BRUTA =====")
    all_passed = True
    
    TABLE_NAME = "MV_OLAP_CUBE_KPIs"
    # Usamos la clave "agg_cube"
    expected = EXPECTED_SCHEMA["agg_cube"] 
    
    try:
        df_agg = pl.read_database(f"SELECT * FROM {TABLE_NAME}", connection=engine)
        
        # PRUEBA Conteo de Filas (Validando el Cubo Agregado)
        if df_agg.shape[0] < expected["min_rows"]:
            print(f"FALLO: Tabla {TABLE_NAME} tiene {df_agg.shape[0]} filas, menos de {expected['min_rows']} esperadas (Completitud).")
            all_passed = False
        else:
            print(f"PASSED: Tabla {TABLE_NAME} (Agregación) tiene un conteo de filas aceptable: {df_agg.shape[0]}.")
            
        # PRUEBA B: Verificación de Nombres de Columnas (Estructura)
        df_cols = set(df_agg.columns)
        required_cols = set(expected["expected_columns"])
        if not required_cols.issubset(df_cols):
             print(f"FALLO: Faltan columnas clave en {TABLE_NAME}. Faltantes: {required_cols - df_cols}")
             all_passed = False
        else:
             print(f"PASSED: Todas las columnas clave encontradas en {TABLE_NAME}.")

    except Exception as e:
        print(f"ERROR FATAL: No se pudo leer la tabla {TABLE_NAME}. Error: {e}")
        all_passed = False
        
    print("\n===== FIN DE LA VALIDACIÓN =====")
    return all_passed

# --- EJECUCIÓN ---

if __name__ == "__main__":
    if not os.path.exists(DW_PATH):
        print(f"ERROR: Base de datos no encontrada en {DW_PATH}. Ejecute el procesador de datos primero.")
    else:
        engine = create_engine(DW_CONNECTION_STRING)
        if validate_extraction_completeness(engine):
            print("\nRESULTADO GLOBAL: Todas las pruebas de validación pasaron.")
        else:
            print("\nRESULTADO GLOBAL: Fallaron algunas pruebas de validación. ¡Revisar la extracción!")