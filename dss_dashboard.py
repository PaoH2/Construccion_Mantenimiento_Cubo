import streamlit as st
import requests
import pandas as pd
from typing import Optional, Dict, Any, List

# --- CONFIGURACIÓN DE LA APLICACIÓN ---
API_BASE_URL = "http://127.0.0.1:8000"
DSS_ACCESS_TOKEN = "Bearer DSS-Access-Token"

# Inicializar el estado de autenticación
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

st.set_page_config(layout="wide", page_title="DSS - Balanced Scorecard")

# --- FUNCIÓN DE UTILIDAD: LLAMADAS AL API ---
def fetch_api(endpoint: str, method: str = "GET", params: Optional[Dict[str, Any]] = None, token: Optional[str] = None) -> List[Dict[str, Any]] | Dict[str, Any]:
    """Función genérica para hacer llamadas a la API de FastAPI, manejando autenticación y errores."""
    headers = {}
    if token:
        headers["Authorization"] = token
    headers["Content-Type"] = "application/json"
    url = f"{API_BASE_URL}{endpoint}"
    
    response = None
    try:
        if method == "GET":
            response = requests.get(url, params=params, headers=headers)
        
        response.raise_for_status() # Lanza una excepción para errores 4xx/5xx
        return response.json()
    except requests.exceptions.RequestException as e:
        response_status = getattr(response, 'status_code', None)
        
        if response_status == 403: st.error("Acceso Denegado. Token inválido.")
        elif response_status == 404: st.warning("No se encontraron datos para los filtros aplicados (404).")
        elif response_status == 503: st.error("Servicio Analítico No Disponible (503). Verifique que el servidor FastAPI esté corriendo.")
        else: st.error(f"Error de Conexión: Verifique si el servidor FastAPI está corriendo en {API_BASE_URL}.")
        return []

# --- MÓDULOS DEL DASHBOARD ---

def get_color(value, threshold, is_index=True):
    """Determina el color de riesgo para Streamlit (inverse es rojo/malo)."""
    if is_index: # CPI/SPI: Riesgo si < 0.95
        return "inverse" if value < threshold else "normal"
    else: # SV/Densidad: Riesgo si < 0 o > 0.15
        return "inverse" if value < threshold else "normal" if value > threshold else "normal"

def login_page():
    """Muestra la página de login y maneja la autenticación."""
    st.sidebar.header("Acceso al Scorecard")
    st.title("Bienvenido al DSS de Gestión de Proyectos")

    token_input = st.sidebar.text_input("Ingrese su Token de Acceso (DSS-Access-Token)")
    
    if st.sidebar.button("Iniciar Sesión"):
        if token_input == DSS_ACCESS_TOKEN:
            st.session_state.authenticated = True
            st.session_state.dss_token = token_input
            st.rerun() 
        else:
            st.sidebar.error("Token Inválido. Acceso Denegado.")

def static_diagram_page():
    """
    Muestra los diagramas estáticos de las 6 métricas globales estáticas.
    """
    # 6 MÉTRICAS ESTÁTICAS GLOBALES (KPIs y Hechos)
    
    col_static_1, col_static_2, col_static_3 = st.columns(3)
    col_static_4, col_static_5, col_static_6 = st.columns(3)
    
    # Fila 1: Hechos (Valores Absolutos)
    col_static_1.metric(
        label="Horas Reales Invertidas (SUM(Horas Reales))",
        value="14,500", 
        help="Total de esfuerzo registrado."
    )
    col_static_2.metric(
        label="Costo Total Gastado (SUM(Costo Real))",
        value="$725,000", 
        help="Costo financiero total de la actividad."
    )
    col_static_3.metric(
        label="Desviación de Cronograma (SV Global)",
        value="-$12,500", 
        delta="-$12,500", 
        delta_color="inverse", 
        help="SUM(EV - PV). Indica un retraso global."
    )
    
    # Fila 2: Índices (Valores Relativos - Los más importantes para el riesgo)
    col_static_4.metric(
        label="Rendimiento de Costo (CPI Global)",
        value="0.98", 
        delta="-0.02", 
        delta_color="normal",
        help="¿Estamos por debajo del presupuesto (Meta = 1.0)?"
    )
    col_static_5.metric(
        label="Rendimiento de Cronograma (SPI Global)",
        value="1.05", 
        delta="+0.05", 
        delta_color="normal",
        help="¿Estamos adelantados al cronograma (Meta = 1.0)?"
    )
    col_static_6.metric(
        label="Densidad de Defectos (Global)",
        value="0.12", 
        delta="0.00", 
        delta_color="normal",
        help="Tasa de errores por KLOC (Riesgo > 0.15)."
    )
    
    st.markdown("---")
    st.markdown("Visualización de las Métricas Clave")

    # ====================================================================
    # DIAGRAMA 1: Comparativa de Horas y Costo (Hechos)
    # ====================================================================
    st.subheader("1. Comparativa de Inversión (Horas vs. Costo)")
    # Simulación de datos para el gráfico
    chart_data_1 = pd.DataFrame({
        'Métrica': ['Horas', 'Costo'],
        'Valor': [14500, 725000 / 100], # Se escala el costo para que sea visible
    })
    st.bar_chart(chart_data_1.set_index('Métrica')) 

    st.markdown("---")
    
    # ====================================================================
    # DIAGRAMA 2: Índice de Rendimiento vs. Riesgo (CPI vs SPI)
    # ====================================================================
    st.subheader("2. Rendimiento de Índices (CPI vs SPI)")
    # Simulación de datos de CPI y SPI a lo largo del tiempo
    chart_data_2 = pd.DataFrame({
        'Mes': ['Ene', 'Feb', 'Mar', 'Abr', 'May'],
        'CPI': [1.02, 1.01, 0.96, 0.98, 0.95],
        'SPI': [0.95, 0.98, 1.05, 1.00, 1.03],
    })
    chart_data_2 = chart_data_2.set_index('Mes')
    st.line_chart(chart_data_2) 
    
    st.markdown("---")
    
    # ====================================================================
    # DIAGRAMA 3: Desviación (SV) y Calidad
    # ====================================================================
    st.subheader("3. Desviación Acumulada (SV) y Densidad de Defectos")
    
    # Simulación de datos de SV y Densidad
    chart_data_3 = pd.DataFrame({
        'Métrica': ['Desviación (SV)', 'Riesgo (Densidad)'],
        'Valor': [-12500, 12000], # Simulación: SV negativo vs. Densidad escalada
    })
    st.bar_chart(chart_data_3.set_index('Métrica'))

    st.markdown("---")

def olap_visualization_module(dss_token: str):
    """Visualización del Cubo OLAP y Gestión de Valor Ganado (EVM) con datos dinámicos."""
    st.title("Balanced Scorecard Dinámico")
    
    # --- LLAMADA A LA API PARA OBTENER DATOS ---
    dimensions_data = fetch_api(endpoint="/api/olap/dimensions", method="GET", token=dss_token)
    
    if not dimensions_data or 'dimensions' not in dimensions_data:
        st.error("No se pudieron cargar las dimensiones del Cubo. Revise la API.")
        return

    ALL_GROUPINGS = dimensions_data['dimensions']
    
    # --- FILTROS Y DIMENSIONES ---
    st.markdown("##### Configuración de la Vista (Slice, Dice, Drill-Down)")
    
    col1, col2 = st.columns(2)
    
    group_by = col1.selectbox("Agrupar por (Drill-Down / Roll-Up):", ALL_GROUPINGS, index=0)
    
    st.markdown("---")
    st.markdown("###### Filtros Adicionales (Slice & Dice)")
    
    col_filters = st.columns(3)
    
    # Filtros dinámicos basados en las dimensiones disponibles
    filter_anio = col_filters[0].selectbox("Filtrar Año:", ["TODOS", 2024, 2023])
    filter_producto = col_filters[1].selectbox("Filtrar Producto:", ["TODOS", "Producto A", "Producto B", "Producto C"])
    filter_proyecto = col_filters[2].selectbox("Filtrar Proyecto:", ["TODOS", "Proyecto Alpha", "Proyecto Beta", "Proyecto Gamma"])
    
    # --- PREPARACIÓN DE PARÁMETROS Y LLAMADA A LA API ---
    params = {
        "group_by_dimension": group_by,
        "anio": filter_anio if filter_anio != "TODOS" else None,
        "producto": filter_producto if filter_producto != "TODOS" else None,
        "proyecto": filter_proyecto if filter_proyecto != "TODOS" else None,
    }

    olap_data = fetch_api(endpoint="/api/olap/query", method="GET", params=params, token=dss_token)
    
    # --- VISUALIZACIÓN DE RESULTADOS ---
    if olap_data:
        df_olap = pd.DataFrame(olap_data)
        
        # Renombrar columnas para la visualización
        df_olap.columns = [c.replace('_promedio', '_p').replace('_sum', '_s') for c in df_olap.columns]
        
        # Cálculo de KPIs Globales para las tarjetas (KPIs de Nivel 1)
        global_cpi = df_olap["cpi_index_p"].mean()
        global_spi = df_olap["spi_index_p"].mean()
        global_sv = df_olap["schedule_variance_s"].sum()
        global_density = df_olap["densidad_defectos_p"].mean()

        st.markdown("### KPIs Globales de EVM y Calidad")
        col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)
        
        # 1. CPI
        cpi_color = get_color(global_cpi, 0.95)
        col_kpi1.metric("CPI (Eficiencia Costo)", f"{global_cpi:.2f}", delta="0.00", delta_color=cpi_color, help="< 0.95: Sobrecosto Crítico")

        # 2. SPI
        spi_color = get_color(global_spi, 0.95)
        col_kpi2.metric("SPI (Eficiencia Cronograma)", f"{global_spi:.2f}", delta="0.00", delta_color=spi_color, help="< 0.95: Retraso Significativo")
        
        # 3. SV
        sv_color = get_color(global_sv, 0, is_index=False)
        col_kpi3.metric("SV (Desviación Cronograma)", f"${global_sv:,.0f}", delta="0", delta_color=sv_color, help="< $0: Déficit de Entrega")

        # 4. Densidad
        density_color = "inverse" if global_density > 0.15 else "normal"
        col_kpi4.metric("Densidad de Defectos (x/KLOC)", f"{global_density:.3f}", delta="0.00", delta_color=density_color, help="> 0.15: Calidad Comprometida")
        
        st.markdown("---")
        st.subheader(f"Desglose por Agrupación: {group_by}")

        # VISUALIZACIÓN DETALLADA (Tabla y Gráfico)
        dimension_col = df_olap.columns[0] 
        df_olap_indexed = df_olap.set_index(dimension_col)
        
        st.dataframe(df_olap, use_container_width=True)
        
        # Gráfico: Comparativa de Índices (CPI vs SPI)
        st.subheader("Tendencia y Comparativa de Índices de Rendimiento (CPI vs SPI)")
        st.line_chart(df_olap_indexed[["cpi_index_p", "spi_index_p"]])


def main_app():
    """Función principal del DSS con control de vista estática/dinámica."""
    st.sidebar.title("Acceso al DSS")
    
    # Control de vista: Si no está autenticado, muestra el login y la vista estática.
    if not st.session_state.authenticated:
        login_page()
        static_diagram_page() 
    else:
        # Si está autenticado, muestra el dashboard dinámico.
        olap_visualization_module(st.session_state.dss_token)

if __name__ == "__main__":
    main_app()