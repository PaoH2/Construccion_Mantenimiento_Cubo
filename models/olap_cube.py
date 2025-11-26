import polars as pl
from typing import List, Dict, Any, Optional

class OlapCube:
    """
    Clase que encapsula el DataFrame de Polars pre-agregado (el Cubo OLAP).
    Define la lógica de negocio (Jerarquías y Agregaciones) para los KPIs de EVM.
    
    ASUME que las columnas del DataFrame están en minúsculas (ej. 'proyecto').
    """
    
    # DEFINICIÓN DE LAS JERARQUÍAS DISPONIBLES (Sin Región)
    # Clave (PascalCase, lo que el usuario/URL envía) -> Valor (minúscula, lo que Polars necesita)
    HIERARCHY_MAP = {
        "Anio": ["anio"],
        "Producto": ["producto"],
        "Proyecto": ["proyecto"],
        "Anio_Producto": ["anio", "producto"],
        "Proyecto_Anio": ["proyecto", "anio"],
    }
    
    # Medidas válidas para la agregación (KPIs de EVM y Calidad)
    VALID_MEASURES_NAMES = [
        "cpi_index_promedio", 
        "spi_index_promedio",
        "schedule_variance_sum",
        "densidad_defectos_promedio"
    ]
    
    def __init__(self, data_frame: pl.DataFrame):
        """Inicializa la clase con el DataFrame de Polars cargado en memoria."""
        if data_frame.is_empty():
            raise ValueError("El DataFrame del cubo no puede estar vacío.")
        self.df_base = data_frame
        
    def olap_query(
        self,
        group_by_dimension: str,
        # Filtros de dimensión (Region fue eliminada)
        anio: Optional[int] = None,
        producto: Optional[str] = None,
        proyecto: Optional[str] = None,
    ) -> pl.DataFrame:
        """
        Ejecuta operaciones OLAP con filtros (Slice/Dice) y agrupamiento (Drill-Down).
        """
        
        # Hardening: Limpieza de la dimensión recibida (elimina caracteres invisibles)
        cleaned_dimension = group_by_dimension.strip().replace('\ufeff', '').replace('\xa0', '')
        
        # 1. VERIFICAR Y RESOLVER LA JERARQUÍA
        if cleaned_dimension not in self.HIERARCHY_MAP:
            raise ValueError(f"Jerarquía de agrupación inválida. Use una de: {list(self.HIERARCHY_MAP.keys())}")
            
        groups = self.HIERARCHY_MAP[cleaned_dimension]

        # 2. Clonar y Pre-procesar (CASTING DE TIPOS)
        df = self.df_base.clone()
        
        # Corrección de Tipo: Forzar las columnas KPI a Float64 antes de la agregación
        try:
            df = df.with_columns([
                pl.col(kpi).cast(pl.Float64) for kpi in self.VALID_MEASURES_NAMES
            ])
        except pl.ColumnNotFoundError as e:
             # Este error no debería ocurrir si el procesador guardó las columnas correctamente
             raise ValueError(f"Error interno: Columna KPI no encontrada ({e}). Revise el procesador OLAP.")

        # 3. Aplicar Filtros (SLICE y DICE) - Usamos nombres de columna en minúscula
        filter_conditions = []
        
        if anio:
            filter_conditions.append(pl.col("anio") == anio)
        if producto:
            filter_conditions.append(pl.col("producto") == producto)
        if proyecto:
            filter_conditions.append(pl.col("proyecto") == proyecto)

        if filter_conditions:
            combined_filter = filter_conditions[0]
            for condition in filter_conditions[1:]:
                combined_filter = combined_filter & condition
            df = df.filter(combined_filter)

        # 4. Definir y Realizar Agregación (DRILL-DOWN y ROLL-UP)
        aggregations = [
            pl.mean("cpi_index_promedio").alias("cpi_index_promedio"),
            pl.mean("spi_index_promedio").alias("spi_index_promedio"),
            pl.sum("schedule_variance_sum").alias("schedule_variance_sum"), 
            pl.mean("densidad_defectos_promedio").alias("densidad_defectos_promedio"),
        ]
        
        df_result = df.group_by(groups).agg(aggregations).sort(groups)
        
        return df_result