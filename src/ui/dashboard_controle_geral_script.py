import datetime as dt

import pandas as pd
import plotly.express as px
import streamlit as st
from html import escape
from sqlalchemy import text

from src.db.engine import Base, engine, get_db
from src.db.seed import seed_db
from src.services.kpis_service import KPIService

st.markdown(
    """
<style>
    html, body, [class*="css"] {
        font-family: "Segoe UI", Tahoma, Arial, sans-serif;
    }
    .kpi-card {
        background: var(--secondary-background-color);
        border: 1px solid rgba(127, 127, 127, 0.28);
        border-radius: 10px;
        padding: 14px 16px;
        height: 132px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        margin: 0;
    }
    .kpi-head {
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .kpi-icon {
        width: 24px;
        height: 24px;
        border-radius: 8px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-size: 0.95rem;
        font-weight: 600;
        line-height: 1;
        font-family: "Segoe UI Emoji", "Segoe UI Symbol", "Segoe UI", Tahoma, Arial, sans-serif;
    }
    .kpi-title {
        color: var(--text-color);
        opacity: 0.86;
        font-size: 0.95rem;
        font-weight: 600;
        line-height: 1.2;
    }
    .kpi-value {
        color: var(--text-color);
        font-size: 2.05rem;
        font-weight: 700;
        line-height: 1.05;
        letter-spacing: 0.2px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .kpi-foot {
        min-height: 24px;
        display: flex;
        align-items: center;
    }
    .kpi-badge {
        display: inline-block;
        border-radius: 999px;
        padding: 3px 10px;
        font-size: 0.83rem;
        font-weight: 600;
    }
    .kpi-badge-neutral {
        color: #c7d2fe;
        background: rgba(99, 102, 241, 0.18);
    }
    .kpi-badge-good {
        color: #86efac;
        background: rgba(34, 197, 94, 0.16);
    }
    .kpi-badge-warn {
        color: #fcd34d;
        background: rgba(245, 158, 11, 0.18);
    }
    .kpi-badge-danger {
        color: #fda4af;
        background: rgba(239, 68, 68, 0.18);
    }
    .kpi-tone-info {
        border-color: rgba(100, 116, 139, 0.5);
        background: linear-gradient(180deg, rgba(59, 130, 246, 0.10) 0%, var(--secondary-background-color) 80%);
    }
    .kpi-tone-info .kpi-icon {
        color: #93c5fd;
        background: rgba(59, 130, 246, 0.2);
    }
    .kpi-tone-good {
        border-color: rgba(22, 163, 74, 0.55);
        background: linear-gradient(180deg, rgba(34, 197, 94, 0.10) 0%, var(--secondary-background-color) 80%);
    }
    .kpi-tone-good .kpi-icon {
        color: #22c55e;
        background: rgba(22, 163, 74, 0.24);
    }
    .kpi-tone-warn {
        border-color: rgba(245, 158, 11, 0.58);
        background: linear-gradient(180deg, rgba(245, 158, 11, 0.11) 0%, var(--secondary-background-color) 80%);
    }
    .kpi-tone-warn .kpi-icon {
        color: #f59e0b;
        background: rgba(245, 158, 11, 0.24);
    }
    .kpi-tone-danger {
        border-color: rgba(239, 68, 68, 0.58);
        background: linear-gradient(180deg, rgba(239, 68, 68, 0.11) 0%, var(--secondary-background-color) 80%);
    }
    .kpi-tone-danger .kpi-icon {
        color: #ef4444;
        background: rgba(239, 68, 68, 0.24);
    }
    .kpi-tone-cashin {
        border-color: rgba(6, 182, 212, 0.55);
        background: linear-gradient(180deg, rgba(6, 182, 212, 0.11) 0%, var(--secondary-background-color) 80%);
    }
    .kpi-tone-cashin .kpi-icon {
        color: #06b6d4;
        background: rgba(6, 182, 212, 0.24);
    }
    .kpi-tone-cashout {
        border-color: rgba(139, 92, 246, 0.55);
        background: linear-gradient(180deg, rgba(139, 92, 246, 0.11) 0%, var(--secondary-background-color) 80%);
    }
    .kpi-tone-cashout .kpi-icon {
        color: #8b5cf6;
        background: rgba(139, 92, 246, 0.24);
    }
    .kpi-row-space {
        height: 14px;
    }
    section[data-testid="stSidebar"] [data-baseweb="tag"] {
        background: linear-gradient(
            90deg,
            rgba(6, 182, 212, 0.28),
            rgba(59, 130, 246, 0.30)
        ) !important;
        border: 1px solid rgba(34, 211, 238, 0.65) !important;
    }
    section[data-testid="stSidebar"] [data-baseweb="tag"] span {
        color: #cffafe !important;
        font-weight: 600 !important;
    }
    section[data-testid="stSidebar"] [data-baseweb="tag"] svg {
        fill: #cffafe !important;
    }
    h3 {
        color: var(--text-color) !important;
    }
</style>
""",
    unsafe_allow_html=True,
)

if "db_initialized" not in st.session_state:
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT count(*) FROM materiais")).scalar()
            if res == 0:
                st.info("Banco vazio. Carregando planilha de referencia...")
                seed_db()
        st.session_state["db_initialized"] = True
    except Exception:
        Base.metadata.create_all(bind=engine)
        seed_db()
        st.session_state["db_initialized"] = True

db = next(get_db())
service = KPIService(db)

all_categories = service.list_categorias()

with st.sidebar:
    st.markdown("### Filtros")
    selected_categories = st.multiselect(
        "Familias",
        options=all_categories,
        default=all_categories,
        help="Selecione familias especificas. Vazio = todas as familias.",
    )

    item_rows = service.list_itens(categorias=selected_categories)
    item_map = {
        row["sku"]: (
            f"{row['sku']} - {row['descricao']}"
            if str(row.get("descricao") or "").strip()
            else row["sku"]
        )
        for row in item_rows
    }
    item_options = list(item_map.keys())
    item_filter_key = "dashboard_filter_skus"
    if item_filter_key not in st.session_state:
        st.session_state[item_filter_key] = []
    elif not isinstance(st.session_state[item_filter_key], list):
        st.session_state[item_filter_key] = list(st.session_state[item_filter_key])

    st.session_state[item_filter_key] = [
        sku for sku in st.session_state[item_filter_key] if sku in item_options
    ]

    selected_skus = st.multiselect(
        "Itens",
        options=item_options,
        key=item_filter_key,
        format_func=lambda sku: item_map.get(sku, sku),
        help="Selecione item a item. Vazio = todos os itens.",
    )

    min_date, max_date = service.get_date_bounds(
        categorias=selected_categories,
        skus=selected_skus,
    )
    date_filter_key = "dashboard_filter_periodo"
    default_start = max(min_date, max_date - dt.timedelta(days=30))
    default_period = (default_start, max_date)

    if date_filter_key not in st.session_state:
        st.session_state[date_filter_key] = default_period
    else:
        current_period = st.session_state.get(date_filter_key)
        if isinstance(current_period, (tuple, list)):
            if len(current_period) >= 2:
                current_start, current_end = current_period[0], current_period[1]
            elif len(current_period) == 1:
                current_start = current_end = current_period[0]
            else:
                current_start, current_end = default_period
        else:
            current_start = current_end = current_period

        current_start = max(min_date, min(current_start, max_date))
        current_end = max(min_date, min(current_end, max_date))
        if current_start > current_end:
            current_start, current_end = current_end, current_start
        st.session_state[date_filter_key] = (current_start, current_end)

    periodo = st.date_input(
        "Periodo",
        min_value=min_date,
        max_value=max_date,
        key=date_filter_key,
        help="Controla o periodo de todos os indicadores e graficos.",
    )
    if isinstance(periodo, (tuple, list)):
        if len(periodo) == 2:
            selected_start, selected_end = periodo
        elif len(periodo) == 1:
            selected_start = selected_end = periodo[0]
        else:
            selected_start, selected_end = min_date, max_date
    else:
        selected_start = selected_end = periodo

st.title("Dashboard Executivo de Estoque - Agricola")
st.markdown("Base de dados: **Planilha Controle de Insumos Agricolas**")


def format_curr(val):
    val = float(val or 0)
    formatted = f"{val:,.2f}"
    formatted = formatted.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R${formatted}"


def format_decimal(val, dec=1):
    val = float(val or 0)
    formatted = f"{val:,.{dec}f}"
    return formatted.replace(",", "X").replace(".", ",").replace("X", ".")


def render_kpi_card(
    container,
    title,
    value,
    badge_text=None,
    badge_style="neutral",
    tone="info",
    icon="",
):
    badge_html = "&nbsp;"
    if badge_text:
        badge_html = (
            f'<span class="kpi-badge kpi-badge-{escape(badge_style)}">'
            f"{escape(str(badge_text))}</span>"
        )
    icon_html = (
        f'<span class="kpi-icon">{escape(str(icon))}</span>' if str(icon).strip() else ""
    )

    card_html = (
        f'<div class="kpi-card kpi-tone-{escape(str(tone))}">'
        f'<div class="kpi-head">{icon_html}<div class="kpi-title">{escape(str(title))}</div></div>'
        f'<div class="kpi-value">{escape(str(value))}</div>'
        f'<div class="kpi-foot">{badge_html}</div>'
        "</div>"
    )
    container.markdown(card_html, unsafe_allow_html=True)


try:
    data = service.get_dashboard_summary(
        categorias=selected_categories,
        skus=selected_skus,
        date_start=selected_start,
        date_end=selected_end,
    )
except Exception as e:
    st.error(f"Erro ao calcular KPIs: {e}")
    if st.button("Tentar corrigir dados"):
        seed_db()
        st.rerun()
    st.stop()

if not data:
    st.warning("Sem dados para exibir.")
    st.stop()

c1, c2, c3, c4 = st.columns(4, gap="medium")
render_kpi_card(
    c1,
    "Ruptura do Estoque",
    f"{int(data['ruptura'])} itens",
    "Critico" if data["ruptura"] > 0 else "Ok",
    "danger" if data["ruptura"] > 0 else "good",
    icon="🛑",
)
render_kpi_card(
    c2,
    "Abaixo do Minimo",
    f"{int(data['abaixo_min'])} itens",
    "Atencao" if data["abaixo_min"] > 0 else "Controlado",
    "warn" if data["abaixo_min"] > 0 else "good",
    icon="⚠️",
)
coverage_tone = "danger" if data["cobertura"] < 10 else "warn" if data["cobertura"] < 30 else "info"
render_kpi_card(
    c3,
    "Cobertura Media",
    f"{format_decimal(data['cobertura'])} dias",
    tone=coverage_tone,
    icon="⏱️",
)
render_kpi_card(
    c4,
    "Alertas Ativos",
    f"{int(data['alertas'])}",
    tone="warn" if data["alertas"] > 0 else "good",
    icon="🔔",
)

st.markdown('<div class="kpi-row-space"></div>', unsafe_allow_html=True)

c5, c6, c7, c8 = st.columns(4, gap="medium")
render_kpi_card(
    c5,
    "Val. Entrada (Total)",
    format_curr(data["val_entrada"]),
    tone="cashin",
    icon="📥",
)
render_kpi_card(
    c6,
    "Val. Saida (Total)",
    format_curr(data["val_saida"]),
    tone="cashout",
    icon="📤",
)
render_kpi_card(
    c7,
    "Val. Estoque Atual",
    format_curr(data["val_estoque"]),
    tone="info",
    icon="💰",
)
render_kpi_card(
    c8,
    "N. Pendencias",
    f"{int(data['pendencias'])}",
    "Alta Prioridade" if data["pendencias"] > 0 else "Sem Pendencias",
    "danger" if data["pendencias"] > 0 else "good",
    icon="📌",
)

st.divider()

col_left, col_right = st.columns([2, 1])

with col_left:
    st.markdown("### Valor de Entradas")
    df_ent = pd.DataFrame(data.get("series_entradas", []))

    if not df_ent.empty:
        df_ent.columns = ["Data", "Valor Entrada"]
        df_ent["Valor Formatado"] = df_ent["Valor Entrada"].apply(format_curr)
        fig_ent = px.bar(
            df_ent,
            x="Data",
            y="Valor Entrada",
            custom_data=["Valor Formatado"],
            color_discrete_sequence=["#00E5FF"],
        )
        fig_ent.update_traces(
            hovertemplate="Data: %{x|%d/%m/%Y}<br>Valor: %{customdata[0]}<extra></extra>"
        )
        fig_ent.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            height=300,
            xaxis_title=None,
            yaxis_title="Valor (R$)",
            bargap=0.12,
            separators=",.",
        )
        fig_ent.update_xaxes(tickformat="%d/%m", dtick=86400000)
        fig_ent.update_yaxes(tickprefix="R$", tickformat=",.2f")
        st.plotly_chart(fig_ent, use_container_width=True, theme="streamlit")
    else:
        st.info("Sem dados de entradas recentes.")

    st.markdown("### Valor de Saidas")
    df_out = pd.DataFrame(data.get("series_saidas", []))

    if not df_out.empty:
        df_out.columns = ["Data", "Valor Saida"]
        df_out["Valor Formatado"] = df_out["Valor Saida"].apply(format_curr)
        fig_out = px.bar(
            df_out,
            x="Data",
            y="Valor Saida",
            custom_data=["Valor Formatado"],
            color_discrete_sequence=["#FF1744"],
        )
        fig_out.update_traces(
            hovertemplate="Data: %{x|%d/%m/%Y}<br>Valor: %{customdata[0]}<extra></extra>"
        )
        fig_out.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            height=300,
            xaxis_title=None,
            yaxis_title="Valor (R$)",
            bargap=0.12,
            separators=",.",
        )
        fig_out.update_xaxes(tickformat="%d/%m", dtick=86400000)
        fig_out.update_yaxes(tickprefix="R$", tickformat=",.2f")
        st.plotly_chart(fig_out, use_container_width=True, theme="streamlit")
    else:
        st.info("Sem dados de saidas recentes.")

with col_right:
    st.markdown("### Consumo por Familia (R$)")
    if not isinstance(data.get("bar_familia"), pd.DataFrame):
        df_fam = pd.DataFrame(data["bar_familia"])
    else:
        df_fam = data["bar_familia"]

    if not df_fam.empty:
        df_fam.columns = ["Categoria", "Valor Consumido"]
        df_fam = df_fam.sort_values("Valor Consumido", ascending=True)
        df_fam["Valor Formatado"] = df_fam["Valor Consumido"].apply(format_curr)

        fig_fam = px.bar(
            df_fam,
            y="Categoria",
            x="Valor Consumido",
            orientation="h",
            text="Valor Formatado",
            custom_data=["Valor Formatado"],
            color_discrete_sequence=["#8EEA9A"],
        )
        fig_fam.update_traces(
            texttemplate="%{text}",
            hovertemplate="Categoria: %{y}<br>Valor: %{customdata[0]}<extra></extra>",
        )
        chart_height = max(650, 42 * len(df_fam))
        fig_fam.update_layout(
            margin=dict(l=0, r=0, t=30, b=0),
            height=chart_height,
            separators=",.",
        )
        fig_fam.update_xaxes(
            tickprefix="R$",
            tickformat=",.2f",
            title="Valor Consumido (R$)",
        )
        fig_fam.update_yaxes(
            categoryorder="array",
            categoryarray=df_fam["Categoria"].tolist(),
        )
        st.plotly_chart(fig_fam, use_container_width=True, theme="streamlit")
    else:
        st.info("Sem dados por categoria.")
