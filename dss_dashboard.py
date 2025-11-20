import streamlit as st
import requests
import pandas as pd
from typing import Optional, Dict, Any, List

# --- CONFIGURACIÓN DE LA APLICACIÓN ---
API_BASE_URL = "http://127.0.0.1:8000"
# Tokens de Acceso (Deben coincidir exactamente con dss_api.py)
DSS_ACCESS_TOKEN = "Bearer DSS-Access-Token"
PROJECT_LEAD_TOKEN = "Bearer Project-Lead-Token"

st.set_page_config(layout="wide", page_title="DSS - Balanced Scorecard")

# --- FUNCIÓN DE UTILIDAD: LLAMADAS AL API ---

def fetch_api(endpoint: str, method: str = "GET", params: Optional[Dict[str, Any]] = None, json_data: Optional[Dict[str, Any]] = None, token: Optional[str] = None) -> List[Dict[str, Any]] | Dict[str, Any]:
    """Función genérica para hacer llamadas a la API de FastAPI, manejando autenticación y errores."""
    headers = {}
    if token:
        headers["Authorization"] = token
    headers["Content-Type"] = "application/json"
    
    url = f"{API_BASE_URL}{endpoint}"
    
    try:
        response = None
        if method == "GET":
            response = requests.get(url, params=params, headers=headers, timeout=10) 
        elif method == "POST":
            response = requests.post(url, json=json_data, headers=headers, timeout=10)
        
        response.raise_for_status() # Lanza una excepción para errores 4xx/5xx
        return response.json()
    except requests.exceptions.RequestException as e:
        response_status = getattr(response, 'status_code', None)
        
        # Manejo de errores específicos
        if response_status == 403:
            st.error(f"Acceso Denegado (403): Token inválido o insuficiente para el endpoint '{endpoint}'.")
        elif response_status == 404:
            st.warning(f"No se encontraron datos (404) o el endpoint '{endpoint}' no existe.")
        elif response_status == 503:
            st.error("Servicio Analítico No Disponible (503). Verifique que el Cubo OLAP haya cargado.")
        else:
            st.error(f"Error de Conexión: Verifique si el servidor FastAPI está corriendo en {API_BASE_URL}. Detalle: {e}")
        return []

# --- MÓDULOS DEL DASHBOARD ---

def prediction_module(is_project_lead: bool):
    """Interfaz para el modelo de predicción de defectos."""
    st.header("Herramienta de Predicción de Defectos")
    
    if not is_project_lead:
        st.warning("Esta herramienta está restringida al rol de **Responsable de Proyecto**.")
        return

    with st.form("prediction_form"):
        st.markdown("##### Parámetros del Nuevo Proyecto")
        
        col1, col2, col3 = st.columns(3)
        kloc = col1.number_input("Líneas de Código (KLOC)", min_value=1.0, value=35.0, step=1.0)
        complejidad = col2.slider("Factor de Complejidad (1=Baja, 3=Alta)", min_value=1.0, max_value=3.0, value=1.8, step=0.1)
        esfuerzo = col3.number_input("Esfuerzo Planificado (Persona-Mes)", min_value=1.0, value=8.0, step=0.5)
        
        submitted = st.form_submit_button("Calcular Predicción")

        if submitted:
            payload = {
                "lineas_de_codigo_kloc": kloc,
                "complejidad_media": complejidad,
                "esfuerzo_persona_mes": esfuerzo
            }
            
            # La llamada a la API utiliza el token del Project Lead
            result = fetch_api(
                endpoint="/api/dss/defect_prediction", 
                method="POST", 
                json_data=payload, 
                token=PROJECT_LEAD_TOKEN 
            )
            
            if isinstance(result, dict) and 'n_total_predicho' in result:
                st.success("Predicción de Defectos (Rayleigh)")
                
                col_res1, col_res2, _ = st.columns(3)
                
                col_res1.metric(
                    label="Defectos Totales Esperados", 
                    value=f"{result['n_total_predicho']:.0f}"
                )
                col_res2.metric(
                    label="Defectos Esperados en Fase Final (30%)", 
                    value=f"{result['n_en_fase_final']:.0f}"
                )

def olap_visualization_module(dss_token: str):
    """Visualización del Cubo OLAP y Balanced Scorecard."""
    st.header("Balanced Scorecard y KPIs Analíticos")
    
    # NOMBRES DE COLUMNAS ESPERADAS - Alineados a olap_processor_dss.py
    TASA_DEFECTOS_COL = "Tasa_Defectos_Promedio" 
    TAREAS_TIEMPO_COL = "Porcentaje_Tareas_A_Tiempo_Promedio" 

    # --- FILTROS Y DIMENSIONES ---
    st.markdown("##### Configuración de la Vista (Slice, Dice, Drill-Down)")
    
    # Las agrupaciones disponibles deben coincidir con la lógica del OlapCube
    ALL_GROUPINGS = ["Region", "Anio", "Producto", "Anio_Region", "Region_Producto"]
    
    col1, col2 = st.columns(2)
    
    # DRILL-DOWN / ROLL-UP (Incluye las jerarquías)
    group_by = col1.selectbox("Agrupar por (Drill-Down / Roll-Up):", ALL_GROUPINGS, index=0)
    
    st.markdown("---")
    st.markdown("###### Filtros Adicionales (Slice & Dice)")
    
    col_filters = st.columns(3)
    
    # Nota: Los filtros deben ser solo las dimensiones base
    filter_region = col_filters[0].selectbox("Filtrar Región:", ["TODAS", "Norte", "Sur", "Centro", "Este"])
    filter_anio = col_filters[1].selectbox("Filtrar Año:", ["TODOS", 2024, 2023]) 
    filter_producto = col_filters[2].selectbox("Filtrar Producto:", ["TODOS", "Producto A", "Producto B", "Producto C"])
    
    # --- LLAMADA A LA API ---
    
    params = {
        "group_by_dimension": group_by,
        "region": filter_region if filter_region != "TODAS" else None,
        # Convertir a int si no es "TODOS"
        "anio": int(filter_anio) if filter_anio != "TODOS" else None, 
        "producto": filter_producto if filter_producto != "TODOS" else None,
    }

    # Llamada a la API de consulta OLAP con el token general
    olap_data = fetch_api(endpoint="/api/olap/query", method="GET", params=params, token=dss_token)
    
    # --- VISUALIZACIÓN DE RESULTADOS ---
    if olap_data:
        try:
            df_olap = pd.DataFrame(olap_data)
        except Exception as e:
            st.error(f"Error al convertir datos OLAP a DataFrame. Detalle: {e}")
            return
        
        # LÓGICA DE CORRECCIÓN PARA JERARQUÍAS COMPUESTAS
        # Creamos la columna compuesta si la agrupación lo requiere, ya que la API devuelve las dimensiones base por separado (Anio, Region).
        if group_by == "Anio_Region":
            # Concatena Anio (como str) y Region
            df_olap["Anio_Region"] = df_olap["Anio"].astype(str) + " - " + df_olap["Region"].astype(str)
            
        elif group_by == "Region_Producto":
            # Concatena Region y Producto
            df_olap["Region_Producto"] = df_olap["Region"].astype(str) + " - " + df_olap["Producto"].astype(str)

        # -------------------------------------------------------------------
        
        st.markdown(f"#### Resultados Agrupados por: **{group_by}**")
        # Corrección de depreciación: use_container_width -> width='stretch'
        st.dataframe(df_olap, width='stretch')
        
        # --- VERIFICACIONES DE COLUMNA Y GRÁFICOS ---

        # Tasa de Defectos (Calidad)
        st.subheader("1. Tasa de Defectos (Por Unidad de Código)")
        if TASA_DEFECTOS_COL in df_olap.columns:
            # Esta línea ya no fallará para jerarquías porque la columna se creó arriba
            st.bar_chart(df_olap.set_index(group_by)[TASA_DEFECTOS_COL])
        else:
            st.warning(f"Columna '{TASA_DEFECTOS_COL}' no encontrada. Verifique la respuesta de la API.")
            
        # Porcentaje de Tareas a Tiempo (Productividad)
        st.subheader("2. Porcentaje de Tareas a Tiempo (Productividad)")
        if TAREAS_TIEMPO_COL in df_olap.columns:
            st.area_chart(df_olap.set_index(group_by)[TAREAS_TIEMPO_COL])
        else:
            st.warning(f"Columna '{TAREAS_TIEMPO_COL}' no encontrada. Verifique la respuesta de la API.")
        
        # --- KPIs Globales ---
        col_kpi1, col_kpi2 = st.columns(2)
        
        # Cálculo de Promedios Globales solo si las columnas existen
        if TASA_DEFECTOS_COL in df_olap.columns:
            avg_defect_rate = df_olap[TASA_DEFECTOS_COL].mean() * 100 
            col_kpi1.metric("Promedio Global Tasa de Defectos (%)", f"{avg_defect_rate:.2f}%")
        else:
            col_kpi1.metric("Promedio Global Tasa de Defectos (%)", "N/A")

        if TAREAS_TIEMPO_COL in df_olap.columns:
            avg_on_time = df_olap[TAREAS_TIEMPO_COL].mean() * 100
            col_kpi2.metric("Promedio Global Tareas a Tiempo (%)", f"{avg_on_time:.2f}%")
        else:
            col_kpi2.metric("Promedio Global Tareas a Tiempo (%)", "N/A")


def main_app():
    """Función principal del DSS con simulación de autenticación por rol."""
    st.sidebar.title("Acceso al DSS")
    
    # Simulación de Autenticación / Selección de Rol
    user_role = st.sidebar.radio("Seleccione Rol:", ["Analista", "Responsable de Proyecto"])
    
    if user_role == "Responsable de Proyecto":
        is_project_lead = True
        token_to_use = PROJECT_LEAD_TOKEN # El lead tiene acceso a todo
        st.sidebar.success("Acceso Total (Lead)")
    else:
        is_project_lead = False
        token_to_use = DSS_ACCESS_TOKEN # Solo tiene acceso a datos OLAP
        st.sidebar.info("Acceso OLAP (Analista)")

    st.title("Sistema de Soporte de Decisión (DSS) - Desarrollo de Software")
    
    # Módulo OLAP y BSC (Siempre disponible si el token es válido)
    olap_visualization_module(token_to_use)
    
    st.markdown("---")
    
    # Módulo de Predicción (Solo visible si es Responsable de Proyecto)
    prediction_module(is_project_lead)

if __name__ == "__main__":
    main_app()