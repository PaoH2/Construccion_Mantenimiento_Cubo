import polars as pl
from fastapi import FastAPI, HTTPException, APIRouter, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine
import numpy as np
import unicodedata 
from typing import List, Optional, Dict, Any

# --- Importar la clase del Cubo OLAP ---
from models.olap_cube import OlapCube

# --- TOKENS Y CONFIGURACIÓN ---
# Nota: DSS_ACCESS_TOKEN se usa para el Dashboard. PROJECT_LEAD_TOKEN se mantiene si el otro equipo lo requiere.
DSS_ACCESS_TOKEN = "Bearer DSS-Access-Token"
PROJECT_LEAD_TOKEN = "Bearer Project-Lead-Token"

DW_CONNECTION_STRING = "mysql+pymysql://root:@127.0.0.1:3306/Gestion_Proyectos"
AGGREGATION_TABLE_NAME = "MV_OLAP_CUBE_KPIs"
DSS_ACCESS_TOKEN = "Bearer DSS-Access-Token"

# Definición de las dimensiones disponibles (Sin 'region')
AVAILABLE_DIMENSIONS = ["anio", "producto", "proyecto"]

# Inicialización del caché del cubo OLAP (Polars DataFrame)
olap_cube_df: pl.DataFrame = pl.DataFrame()

app = FastAPI(
    title="DSS Gestión de Proyectos API",
    description="Capa Analítica para el Dashboard de Gestión de Proyectos (EVM)."
)
router = APIRouter(prefix="/api/olap")

# Configuración CORS para que Streamlit (puerto 8501) pueda acceder a FastAPI (puerto 8000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501", "http://127.0.0.1:8501"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def load_olap_cube():
    """Carga la Vista Materializada del Cubo OLAP desde MySQL al inicio de la API."""
    global olap_cube_df
    print("INFO: Conectando a DW y cargando tabla MV_OLAP_CUBE_KPIs...")
    try:
        engine = create_engine(DW_CONNECTION_STRING)
        olap_cube_df = pl.read_database(query=f"SELECT * FROM {AGGREGATION_TABLE_NAME}", connection=engine)
        
        # NORMALIZACIÓN DE COLUMNAS A MINÚSCULAS PARA CONSISTENCIA INTERNA
        olap_cube_df.columns = [c.lower() for c in olap_cube_df.columns]
        
        print(f"INFO: Cubo OLAP base cargado. Filas: {olap_cube_df.shape[0]}. Instancia lista.")
        
    except Exception as e:
        print(f"ERROR: Fallo la carga inicial del Cubo desde el DW. Detalle: {e}")
        olap_cube_df = pl.DataFrame()

@app.on_event("startup")
async def startup_event():
    """Evento que se ejecuta al iniciar la aplicación."""
    load_olap_cube()

# ====================================================================
# ENDPOINTS
# ====================================================================

@router.get("/dimensions")
def get_dimensions():
    """Devuelve la lista de dimensiones disponibles."""
    # Retorna las claves de jerarquía que el frontend puede usar (ej., 'Anio', 'Proyecto')
    return {"dimensions": list(OlapCube.HIERARCHY_MAP.keys())}

@router.get("/query")
def query_olap_cube(
    group_by_dimension: str = Query("Proyecto", description="Dimensión para agrupar (Drill-Down)."), 
    kpi_metric: str = Query("cpi_index_promedio", description="Métrica de KPI a mostrar."),
    authorization: Optional[str] = Header(None),
    # Quitamos el parámetro 'region'
    anio: Optional[int] = Query(None, description="Filtra por un año específico."),
    producto: Optional[str] = Query(None, description="Filtra por un producto específico."),
    proyecto: Optional[str] = Query(None, description="Filtra por un proyecto específico."),
):
    """
    Permite consultar el cubo OLAP agrupando por una dimensión y calculando KPIs.
    """
    
    # 1. AUTENTICACIÓN
    if authorization != DSS_ACCESS_TOKEN:
        raise HTTPException(status_code=403, detail="Acceso Denegado.")
    
    if olap_cube_df.is_empty():
        raise HTTPException(
            status_code=503,
            detail="Servicio no disponible. El cubo OLAP no está cargado."
        )

    # 2. LIMPIEZA Y VALIDACIÓN DE LA DIMENSIÓN RECIBIDA
    cleaned_dimension = unicodedata.normalize('NFKD', group_by_dimension).strip()

    # 3. USO DE LA CLASE OLAPCUBE
    try:
        # La clase OlapCube asume que el DF ya está en minúsculas
        cube_instance = OlapCube(olap_cube_df)
        
        # La validación de la jerarquía ocurre dentro del método olap_query de la clase
        df_result = cube_instance.olap_query(
            group_by_dimension=cleaned_dimension,
            # Quitamos el parámetro region en el llamado
            anio=anio,
            producto=producto,
            proyecto=proyecto
        )
        
        # 4. TRADUCCIÓN DE RESULTADO Y RESPUESTA
        if df_result.is_empty():
            raise HTTPException(status_code=404, detail="No se encontraron datos.")
            
        # Retornar el DataFrame de Polars como una lista de diccionarios (JSON)
        return df_result.to_dicts()

    except ValueError as e:
        # Captura el error de 'Jerarquía inválida' de OlapCube
        raise HTTPException(status_code=400, detail=f"Parámetro Inválido: {e}") 
    except Exception as e:
        # Captura errores internos de Polars (KeyError, etc.)
        raise HTTPException(status_code=500, detail=f"Error interno al procesar la consulta OLAP: {e}")

app.include_router(router)