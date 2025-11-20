from fastapi import FastAPI, HTTPException, Header, Query
from pydantic import BaseModel
import numpy as np
import polars as pl
from sqlalchemy import create_engine
from typing import Optional, List, Dict, Any

# --- Importar la clase del Cubo OLAP ---
# Se asume que el archivo olap_cube.py está en la carpeta models/
from models.olap_cube import OlapCube 

# --- TOKENS Y CONFIGURACIÓN ---
DSS_ACCESS_TOKEN = "Bearer DSS-Access-Token"
PROJECT_LEAD_TOKEN = "Bearer Project-Lead-Token"

DW_CONNECTION_STRING = "sqlite:///data/dss_data_warehouse.db" 
# 🚨 CORRECCIÓN CLAVE: Usar el nuevo nombre de la tabla DSS
AGGREGATION_TABLE_NAME = "MV_OLAP_CUBE_KPIs" 

# INICIALIZACIÓN DE VARIABLES GLOBALES Y FRAMEWORK

olap_cube_instance: Optional[OlapCube] = None 

app = FastAPI(
    title="DSS - Capa Analítica",
    description="Endpoints para Cubo OLAP y Modelo Predictivo. (Arquitectura de Microservicio Ligero)"
)

# LÓGICA DE CARGA DE DATOS DESDE EL DW (STARTUP)

@app.on_event("startup")
async def load_data_from_dw():
    global olap_cube_instance
    
    print(f"INFO: Conectando a DW y cargando tabla {AGGREGATION_TABLE_NAME}...")
    
    try:
        engine = create_engine(DW_CONNECTION_STRING)
        query = f"SELECT * FROM {AGGREGATION_TABLE_NAME}"
        
        # Cargar los datos brutos en un DataFrame de Polars
        raw_df = pl.read_database(query=query, connection=engine)
        
        # INSTANCIAR LA CLASE OLAP CON EL DATAFRAME CARGADO
        olap_cube_instance = OlapCube(raw_df)
        
        print(f"INFO: Cubo OLAP base cargado. Filas: {raw_df.shape[0]}. Instancia lista.")
        
    except Exception as e:
        print(f"ERROR: Fallo la carga inicial del Cubo desde el DW. Detalle: {e}")
        olap_cube_instance = None # Deja la instancia como None si falla


# ====================================================================
# 3. MODELOS Y LÓGICA DE PREDICCIÓN (Rayleigh)
# ====================================================================

class PredictionInput(BaseModel):
    lineas_de_codigo_kloc: float
    complejidad_media: float
    esfuerzo_persona_mes: float
    sigma_rayleigh: Optional[float] = 1.5
    factor_base_defectos: Optional[float] = 0.005
    
def predict_rayleigh(data: PredictionInput) -> dict:
    # Lógica del modelo Rayleigh
    riesgo_combinado = (data.complejidad_media * data.esfuerzo_persona_mes) / 100 
    n_base = data.lineas_de_codigo_kloc * data.factor_base_defectos
    
    # Modelo simplificado: D_total = D_base * (1 + (riesgo_combinado / sigma)^2)
    n_total_predicho = n_base * (1 + (riesgo_combinado / data.sigma_rayleigh)**2)
    
    defects_total = int(np.round(n_total_predicho))
    defects_fase_final = int(np.round(n_total_predicho * 0.3)) # Asumiendo 30% de defectos en fase final
    return {
        "n_total_predicho": defects_total,
        "n_en_fase_final": defects_fase_final,
        "lineas_de_codigo_kloc": data.lineas_de_codigo_kloc,
        "estimacion_base_defectos": n_base,
        "modelo_usado": "Distribucion_Rayleigh_Simplificada"
    }


# ====================================================================
# 4. ENDPOINTS DE LA API
# ====================================================================

@app.post("/api/dss/defect_prediction")
def create_defect_prediction(
    input_data: PredictionInput, 
    authorization: Optional[str] = Header(None) 
):
    if authorization != PROJECT_LEAD_TOKEN:
        raise HTTPException(status_code=403, detail="Acceso Denegado.")
    try:
        resultado = predict_rayleigh(input_data)
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al ejecutar el modelo de predicción: {str(e)}")


@app.get("/api/olap/query", response_model=List[Dict[str, Any]])
def olap_query(
    authorization: Optional[str] = Header(None), 
    region: Optional[str] = Query(None, description="Filtra por una región específica."),
    anio: Optional[int] = Query(None, description="Filtra por un año específico."),
    producto: Optional[str] = Query(None, description="Filtra por un producto específico."),
    group_by_dimension: str = Query(..., description="Dimensión o Jerarquía para agrupar (ej. Anio_Region).") 
):
    """
    Simula operaciones OLAP (Slice, Dice, Drill-Down) llamando al método de la clase OlapCube.
    """
    
    if authorization not in [DSS_ACCESS_TOKEN, PROJECT_LEAD_TOKEN]: # Permitir ambos tokens
        raise HTTPException(status_code=403, detail="Acceso Denegado.")
    
    # Uso de la instancia de la clase OlapCube
    if olap_cube_instance is None:
        # 503 Service Unavailable (Temporalmente no disponible)
        raise HTTPException(status_code=503, detail="Servicio no disponible. El Cubo OLAP no está cargado.")

    try:
        # Llamamos al método query del objeto OlapCube, que maneja Slice, Dice y Drill-Down
        df_result = olap_cube_instance.olap_query(
            group_by_dimension=group_by_dimension,
            region=region,
            anio=anio,
            producto=producto
        )
        
        if df_result.is_empty():
            # 404 Not Found (No hay datos para esta combinación de filtros)
            raise HTTPException(status_code=404, detail="No se encontraron datos para los filtros aplicados.")
            
        # Convertir el resultado de Polars a lista de diccionarios (formato JSON estándar)
        return df_result.to_dicts()

    except ValueError as e:
        # Captura el error de 'Jerarquía inválida' de OlapCube
        raise HTTPException(status_code=400, detail=f"Parámetro de consulta inválido: {str(e)}") 
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno al procesar la consulta OLAP: {e}")