import polars as pl
from sqlalchemy import create_engine
import os
from datetime import datetime
import numpy as np
import pandas as pd
from typing import List, Dict, Any

# --- CONFIGURACIÓN ---
# Usaremos SQLite en memoria para simular la conexión.
DW_CONNECTION_STRING = "sqlite:///data/dss_data_warehouse.db" 
# NOMBRE DE TABLA SOLICITADO
AGGREGATION_TABLE_NAME = "MV_OLAP_CUBE_KPIs" 
OUTPUT_DIR = "data"
OUTPUT_FILE_BACKUP = "agregaciones_kpis_backup.parquet"
OUTPUT_PATH_BACKUP = os.path.join(OUTPUT_DIR, OUTPUT_FILE_BACKUP)

# --- SIMULACIÓN DE LA EXTRACCIÓN Y CREACIÓN DEL DATAFRAME BRUTO ---

def create_raw_dss_data() -> pl.DataFrame:
    """Crea los datos brutos de simulación para KPIs de Desarrollo de Software."""
    
    N = 1000 
    date_range = pd.to_datetime(pd.date_range(start='2023-01-01', periods=N // 10, freq='W')).repeat(10)
    if len(date_range) > N:
        date_range = date_range[:N]
    
    df_raw = pl.DataFrame({
        "project_id": np.random.choice(range(100, 110), N),
        "region_dim": np.random.choice(["Norte", "Sur", "Centro", "Este"], N),
        "product_dim": np.random.choice(["Producto A", "Producto B", "Producto C"], N),
        "completion_date": date_range,
        "defects_found": np.random.randint(0, 15, N), 
        "lines_of_code_kloc": np.random.uniform(5, 50, N), 
        "is_on_time": np.random.choice([0, 1], N, p=[0.3, 0.7]), 
        "tasks_completed": np.random.randint(1, 5, N) 
    })
    
    print(f"Datos brutos DSS creados. Filas: {df_raw.shape[0]}")
    return df_raw

# --- PROCESAMIENTO ANALÍTICO Y CREACIÓN DEL CUBO BASE ---

def transform_and_aggregate(df_raw: pl.DataFrame) -> pl.DataFrame:
    """Aplica la lógica analítica para KPIs de DSS y pre-agrega el Cubo OLAP."""
    
    print("Aplicando lógica de negocio y agregación (Polars)...")
    
    # --- Ingeniería de Características y Cálculo a Nivel de Fila ---
    df_processed = df_raw.with_columns([
        pl.col("completion_date").dt.year().alias("Anio"),
        pl.col("region_dim").alias("Region"),
        pl.col("product_dim").alias("Producto"),

        # KPI de Calidad (Tasa de Defectos por KLOC)
        # Fórmula: (Defectos Encontrados / Líneas de Código en KLOC)
        # Esto nos da la densidad de defectos antes de la agregación final.
        (pl.col("defects_found") / pl.col("lines_of_code_kloc")).alias("Defects_Per_KLOC"),
    ])
    
    # --- Agregación (Creación de la Vista Materializada) ---
    df_cube = df_processed.group_by(["Anio", "Region", "Producto"]).agg([
        
        # KPI de Calidad: Tasa de Defectos Promedio
        # Fórmula: Media(Defects_Per_KLOC) por la dimensión agrupada.
        pl.mean("Defects_Per_KLOC").alias("Tasa_Defectos_Promedio"),
        
        # KPI de Productividad: Porcentaje de Tareas a Tiempo
        # Fórmula: Media(is_on_time). Como 'is_on_time' es binario (1/0), la media es el porcentaje.
        pl.mean("is_on_time").alias("Porcentaje_Tareas_A_Tiempo_Promedio"),
        
        # Métricas Absolutas
        pl.sum("tasks_completed").alias("Total_Tareas_Completadas"),
        pl.sum("defects_found").alias("Total_Defectos_Absoluto")
    ])
    
    df_cube = df_cube.with_columns(pl.col("Anio").cast(pl.Int32))
    
    print(f"  -> Procesamiento y Agregación listos. Filas del Cubo: {df_cube.shape[0]}")
    return df_cube

# --- CARGAR EL CUBO EN EL DW COMO TABLA DE AGREGACIÓN ---

def load_cube_to_dw(df_cube: pl.DataFrame, engine):
    """Guarda el DataFrame final en la Tabla de Agregación (DW) y en un archivo Parquet (Backup)."""
    
    print(f"3a. Guardando Cubo en el DW ({AGGREGATION_TABLE_NAME})...")
    
    df_cube_pd = df_cube.to_pandas()
    # Utiliza 'replace' para asegurar que la tabla siempre se crea/actualiza
    df_cube_pd.to_sql(
        name=AGGREGATION_TABLE_NAME, 
        con=engine, 
        if_exists='replace', 
        index=False
    )
    print(f"  -> Carga exitosa en DW.")

    print(f"3b. Guardando Cubo como archivo de lectura rápida ({OUTPUT_FILE_BACKUP})...")
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    df_cube.write_parquet(OUTPUT_PATH_BACKUP)
    print(f"  -> Archivo Parquet generado en {OUTPUT_PATH_BACKUP}.")

# --- FUNCIÓN PRINCIPAL ---

def run_processor():
    """Ejecuta la pipeline completa de ETL/ELT para el Cubo OLAP."""
    
    print(f"\n===== INICIO PROCESADOR OLAP DSS: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====")
    
    try:
        engine = create_engine(DW_CONNECTION_STRING) 

        raw_data = create_raw_dss_data()
        cube_data = transform_and_aggregate(raw_data)
        load_cube_to_dw(cube_data, engine)
        
        print("\n===== PROCESAMIENTO OLAP DSS FINALIZADO CON ÉXITO. =====")
        
    except Exception as e:
        print(f"\nFATAL ERROR en el procesador: {e}")
        
if __name__ == "__main__":
    run_processor()