import polars as pl
from typing import List, Dict, Any, Optional, Union

class OlapCube:
    """
    Clase que encapsula el DataFrame de Polars pre-agregado (el Cubo OLAP) 
    y proporciona métodos para realizar consultas analíticas (Slice, Dice, Drill-Down)
    incluyendo agrupaciones compuestas, ALINEADA A KPIs DE DESARROLLO DE SOFTWARE.
    """
    
    # --- DIMENSIONES Y JERARQUÍAS ---
    BASE_DIMENSIONS = ["Region", "Anio", "Producto"]
    
    HIERARCHIES = {
        "Anio_Region": ["Anio", "Region"],
        "Region_Producto": ["Region", "Producto"]
    }

    # Todas las opciones válidas para el usuario (dashboard)
    VALID_GROUPINGS = BASE_DIMENSIONS + list(HIERARCHIES.keys())
    
    # CORRECCIÓN: Sustitución de KPIs de Ventas por KPIs de DSS 
    # Estos nombres DEBEN COINCIDIR con las columnas generadas en el olap_processor.py
    VALID_MEASURES = [
        # KPI de Calidad (Tasa de Defectos por KLOC) - Se promedia el promedio para el Roll-up
        pl.mean("Tasa_Defectos_Promedio").alias("Tasa_Defectos_Promedio"),
        
        # KPI de Productividad (% de Tareas a Tiempo) - Se promedia el promedio para el Roll-up
        pl.mean("Porcentaje_Tareas_A_Tiempo_Promedio").alias("Porcentaje_Tareas_A_Tiempo_Promedio"),
        
        # Métricas Absolutas - Se suma el total para el Roll-up
        pl.sum("Total_Tareas_Completadas").alias("Total_Tareas_Completadas"),
        pl.sum("Total_Defectos_Absoluto").alias("Total_Defectos_Absoluto")
    ]
    
    # --- CONSTRUCTOR ---
    
    def __init__(self, data_frame: pl.DataFrame):
        if data_frame.is_empty():
            raise ValueError("El DataFrame del cubo no puede estar vacío.")
        self.df_base = data_frame
        
    # --- FUNCIÓN DE CONSULTA OLAP (No necesita cambios funcionales) ---
        
    def olap_query(
        self,
        group_by_dimension: str,
        region: Optional[str] = None,
        anio: Optional[int] = None,
        producto: Optional[str] = None,
    ) -> pl.DataFrame:
        
        # Validación de la Dimensión de Agrupación
        if group_by_dimension not in self.VALID_GROUPINGS:
            raise ValueError(f"Dimensión de agrupación inválida. Use una de: {self.VALID_GROUPINGS}")

        # Determinación de la(s) columna(s) real(es) para Polars
        if group_by_dimension in self.HIERARCHIES:
            group_cols = self.HIERARCHIES[group_by_dimension]
        else:
            group_cols = group_by_dimension
        
        # Filtrado (SLICE y DICE)
        df = self.df_base.clone()
        filter_conditions = []
        
        if region:
            filter_conditions.append(pl.col("Region") == region)
        if anio:
            filter_conditions.append(pl.col("Anio") == anio)
        if producto:
            filter_conditions.append(pl.col("Producto") == producto)

        if filter_conditions:
            combined_filter = filter_conditions[0]
            for condition in filter_conditions[1:]:
                combined_filter = combined_filter & condition
            df = df.filter(combined_filter)

        # Agregación (DRILL-DOWN)
        # Se re-agregan los promedios y totales sobre las nuevas columnas de agrupación
        df_result = df.group_by(group_cols).agg(self.VALID_MEASURES).sort(group_cols)
        
        return df_result