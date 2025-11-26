import polars as pl
from sqlalchemy import create_engine, text
import os
import numpy as np
import pandas as pd
from datetime import datetime
import random

# CONFIGURACIÓN

MYSQL_CONNECTION_STR = "mysql+pymysql://root:@127.0.0.1:3306/Gestion_Proyectos"

DW_CONNECTION_STRING = MYSQL_CONNECTION_STR
AGGREGATION_TABLE_NAME = "mv_olap_cube_kpis" # Minúsculas para compatibilidad MySQL

# Configuración de salida de respaldo (backup)
OUTPUT_DIR = "data"
OUTPUT_FILE_BACKUP = "agregaciones_kpis_backup.parquet"
OUTPUT_PATH_BACKUP = os.path.join(OUTPUT_DIR, OUTPUT_FILE_BACKUP)

def extract_and_transform_data(engine) -> pl.DataFrame:
    """
    Realiza la extracción de datos COMPLETOS de la tabla de hechos y dimensiones de MySQL, 
    y aplica la lógica analítica para calcular los KPIs de EVM/Calidad.
    """
    
    print("2.1. Ejecutando JOIN SQL para Extracción de Datos Brutos...")
    
    # Consulta SQL EXPANDIDA para extraer todas las variables clave del modelo dimensional.
    # Usamos alias en minúsculas y COALESCE para manejar valores NULL/0 en métricas.
    sql_query = f"""
        SELECT 
            -- MÉTRICAS CRÍTICAS DE HECHOS (COALESCE para división segura)
            COALESCE(T1.Costo_Real, 0.0) AS costo_real, 
            COALESCE(T1.EV, 0.0) AS ev, 
            COALESCE(T1.PV, 0.0) AS pv,
            COALESCE(T1.Horas_Reales, 0.0) AS horas_reales,
            COALESCE(T1.Horas_Planificadas, 0.0) AS horas_planificadas,
            COALESCE(T1.SV, 0.0) AS sv_bruto,
            
            -- DIMENSIONES CLAVE
            T3.Anio AS anio,
            T4.Nombre_Proyecto AS proyecto,
            T6.Seniority AS perfil, -- Seniority del Empleado
            T7.Nombre_Tarea AS producto, -- Tarea / Producto
            
            -- OTRAS DIMENSIONES RELEVANTES (Para futuros filtros o análisis de causa raíz)
            T5.Rol AS empleado_rol,
            T9.Metoddologia_Proyecto AS metodologia,
            T10.Estado_Proyecto AS estado_proyecto,
            T11.Tipo_Proyecto AS tipo_proyecto

        FROM Fact_Gestion_Proyecto T1
        
        -- Dimensiones Clave para el Cubo
        INNER JOIN DimAnio T3 ON T1.ID_Anio = T3.ID_Anio
        INNER JOIN DimProyecto T4 ON T1.ID_Proyecto = T4.ID_Proyecto
        INNER JOIN DimEmpleado_Perfil T6 ON T1.ID_Empleado_Perfil = T6.ID_Empleado_Perfil
        INNER JOIN DimTarea T7 ON T1.ID_Tarea = T7.ID_Tarea
        
        -- Dimensiones Adicionales
        INNER JOIN DimEmpleado_Rol T5 ON T1.ID_Empleado_Rol = T5.ID_Empleado_Rol
        INNER JOIN Proyecto_Metodologia T9 ON T1.ID_Proyecto_Metodologia = T9.ID_Proyecto_Metodologia
        INNER JOIN DimProyecto_Estado T10 ON T1.ID_Proyecto_Estado = T10.ID_Proyecto_Estado
        INNER JOIN DimProyecto_Tipo T11 ON T1.ID_Proyecto_Tipo = T11.ID_Proyecto_Tipo;
    """
    
    # 1. EXTRACCIÓN REAL: Polars lee directamente de MySQL
    try:
        df_raw = pl.read_database(query=sql_query, connection=engine)
    except Exception as e:
        print(f"ERROR DE EXTRACCIÓN: Falló la lectura de MySQL: {e}")
        return pl.DataFrame()

    print(f"2.2. Datos extraídos. Filas: {df_raw.shape[0]}. Calculando KPIs...")

    # 2. NORMALIZACIÓN DE COLUMNAS A MINÚSCULAS para consistencia interna (SQL ya las aliasó)
    df_raw.columns = [c.lower() for c in df_raw.columns]
    
    # Casting defensivo: Asegurar que las columnas cruciales sean Float64 para cálculos
    df_raw = df_raw.with_columns([
        pl.col("costo_real").cast(pl.Float64),
        pl.col("ev").cast(pl.Float64),
        pl.col("pv").cast(pl.Float64),
    ])

    # 3. CÁLCULO DE LOS 4 KPIs (EVM y Calidad) con manejo de división por cero
    
    df_transformed = df_raw.with_columns([
        # CPI (Eficiencia de Costo): EV / Costo Real
        pl.when(pl.col("costo_real") == 0.0)
          .then(pl.lit(0.0))
          .otherwise(pl.col("ev") / pl.col("costo_real"))
          .alias("cpi_index"),
        
        # SPI (Eficiencia de Cronograma): EV / PV
        pl.when(pl.col("pv") == 0.0)
          .then(pl.lit(1.0))
          .otherwise(pl.col("ev") / pl.col("pv"))
          .alias("spi_index"),
        
        # SV (Desviación de Cronograma): EV - PV
        pl.col("sv_bruto").alias("schedule_variance"), # Usamos el SV pre-calculado de la tabla de hechos
        
        # Densidad de Defectos (Simulación de un KPI de Calidad - Usaríamos KLOC real si estuviera disponible)
        (pl.lit(np.random.rand(df_raw.shape[0]) * 0.15 + 0.05)).alias("densidad_defectos"), 
        
        # Horas Reales (Para métricas globales estáticas)
        pl.col("horas_reales").alias("horas_reales_total"),
        pl.col("costo_real").alias("costo_real_total"),
    ])
    
    # 4. AGREGACIÓN BASE (Vista Materializada)
    groups = ["anio", "perfil", "proyecto", "producto"] # Grupos al nivel más bajo
    
    df_aggregated = df_transformed.group_by(groups).agg([
        pl.mean("cpi_index").alias("cpi_index_promedio"),
        pl.mean("spi_index").alias("spi_index_promedio"),
        pl.sum("schedule_variance").alias("schedule_variance_sum"),
        pl.mean("densidad_defectos").alias("densidad_defectos_promedio"),
        
        # Métricas Globales Adicionales
        pl.sum("horas_reales_total").alias("horas_reales_sum"),
        pl.sum("costo_real_total").alias("costo_real_sum"),
        
        # Incluir Dimensiones Adicionales para filtros futuros (usando .first() para mantener el valor)
        pl.col("metodologia").first().alias("metodologia"),
        pl.col("estado_proyecto").first().alias("estado_proyecto"),
        pl.col("tipo_proyecto").first().alias("tipo_proyecto"),
        pl.col("empleado_rol").first().alias("empleado_rol"),

    ])

    # 5. La salida final contiene el cubo materializado con todas las métricas y dimensiones.
    return df_aggregated


def load_cube_to_dw(df_cube: pl.DataFrame, engine):
    """Guarda el DataFrame del cubo OLAP en el DW (MySQL) y en un archivo Parquet."""
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # 1. Eliminación de tabla antigua (Solución para if_exists)
    print("3a. Eliminando tabla antigua...")
    try:
        with engine.connect() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {AGGREGATION_TABLE_NAME}"))
            connection.commit()
        print("   -> Tabla antigua eliminada.")
    except Exception as e:
        print(f"   -> ERROR al intentar eliminar la tabla: {e}")

    # 2. GUARDAR EN EL DW (VISTA MATERIALIZADA)
    print("3b. Guardando Cubo en el DW (MV_OLAP_CUBE_KPIs)...")
    try:
        df_cube.write_database(
            table_name=AGGREGATION_TABLE_NAME, 
            connection=engine 
        )
        print("   -> Carga exitosa en DW.")
    except Exception as e:
        print(f"   -> ERROR al guardar en DW: {e}")

    # 3. GUARDAR EN PARQUET (RESPALDO / Lectura Rápida)
    print("3c. Guardando Cubo como archivo de lectura rápida (agregaciones_kpis_backup.parquet)...")
    try:
        df_cube.write_parquet(OUTPUT_PATH_BACKUP)
        print(f"   -> Archivo Parquet generado en {OUTPUT_PATH_BACKUP}.")
    except Exception as e:
        print(f"   -> ERROR al guardar en Parquet: {e}")


def run_processor():
    """Ejecuta el flujo completo de ETL/ELT."""
    print(f"\n INICIO PROCESADOR OLAP: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        db_engine = create_engine(MYSQL_CONNECTION_STR)
    except Exception as e:
        print(f"ERROR: No se pudo crear la conexión a la DB: {e}")
        return

    print("2. Aplicando lógica de negocio y agregación (Polars)...")
    df_cube = extract_and_transform_data(db_engine)
    
    if df_cube.is_empty():
         print("PROCESAMIENTO ABORTADO: No se extrajeron datos de MySQL.")
         return

    print(f"   -> Procesamiento y Agregación listos. Filas del Cubo: {df_cube.shape[0]}")
    
    load_cube_to_dw(df_cube, db_engine)
    
    print(" PROCESAMIENTO OLAP FINALIZADO CON ÉXITO.\n")

if __name__ == "__main__":
    run_processor()