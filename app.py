import streamlit as st

st.set_page_config(page_title="StockPredict", layout="wide", initial_sidebar_state="expanded")

st.markdown(
    """
<style>
section[data-testid="stSidebar"] {
    background: var(--secondary-background-color);
    border-right: 1px solid rgba(127, 127, 127, 0.22);
}
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] {
    padding-top: 0.35rem;
}
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] ul {
    gap: 0.28rem;
}
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] li a {
    border-radius: 10px;
    padding: 0.42rem 0.58rem;
    border: 1px solid transparent;
}
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] li a:hover {
    background: rgba(127, 127, 127, 0.12);
    border-color: rgba(127, 127, 127, 0.28);
}
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] li a[aria-current="page"] {
    background: rgba(56, 189, 248, 0.16);
    border-color: rgba(56, 189, 248, 0.35);
}
section[data-testid="stSidebar"] [data-testid="stSidebarNav"] li a[aria-current="page"] span {
    font-weight: 700;
}
.sidebar-brand {
    margin: 0.1rem 0 0.55rem 0;
    padding: 0.2rem 0.1rem;
}
.sidebar-brand h2 {
    margin: 0;
    color: var(--text-color);
    font-size: 1.08rem;
    letter-spacing: 0.3px;
}
.sidebar-brand p {
    margin: 0.1rem 0 0;
    color: var(--text-color);
    opacity: 0.75;
    font-size: 0.8rem;
}
</style>
""",
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown(
        """
<div class="sidebar-brand">
    <h2>StockPredict</h2>
    <p>Painel de Insumos Agricolas</p>
</div>
""",
        unsafe_allow_html=True,
    )

pages = [
    st.Page("pages/00_Dashboard.py", title="Dashboard", icon=":material/space_dashboard:"),
    st.Page("pages/01_Materiais.py", title="Estoque", icon=":material/inventory_2:"),
    st.Page("pages/02_Previsao.py", title="Previsao", icon=":material/trending_up:"),
    st.Page("pages/03_Alertas.py", title="Alertas", icon=":material/notification_important:"),
    st.Page("pages/05_Assistente.py", title="Assistente", icon=":material/smart_toy:"),
    st.Page("pages/07_Informacoes.py", title="Informacoes", icon=":material/info:"),
]

nav = st.navigation(pages)
nav.run()
