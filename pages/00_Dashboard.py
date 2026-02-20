import streamlit as st

from src.db.engine import get_db
from src.ui.dashboard_controle_geral import render_controle_geral
from src.ui.dashboard_operational_views import (
    render_armazenagem_view,
    render_performance_view,
)


# Navegacao no mesmo padrao visual da pagina de Previsao.
tab_geral, tab_perf, tab_arm = st.tabs(
    ["Controle Geral", "Indicadores de Performance", "Indicadores de Armazenagem"]
)

with tab_geral:
    render_controle_geral()

with tab_perf:
    db_perf = next(get_db())
    render_performance_view(db_perf)

with tab_arm:
    db_arm = next(get_db())
    render_armazenagem_view(db_arm)
