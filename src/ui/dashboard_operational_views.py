import datetime as dt
from html import escape

import pandas as pd
import plotly.express as px
import streamlit as st

from src.services.operational_kpis_service import OperationalKPIsService


CARD_STYLE = """
<style>
    .ops-header {
        border: 1px solid rgba(14, 116, 144, 0.35);
        border-radius: 12px;
        padding: 12px 14px;
        margin-bottom: 0.6rem;
        background: linear-gradient(130deg, rgba(14, 116, 144, 0.12), rgba(15, 23, 42, 0.10));
    }
    .ops-title {
        margin: 0;
        font-size: 1.2rem;
        font-weight: 700;
        letter-spacing: 0.2px;
    }
    .ops-subtitle {
        margin: 0.3rem 0 0;
        opacity: 0.85;
        font-size: 0.9rem;
    }
    .ops-card {
        border-radius: 11px;
        border: 1px solid rgba(127,127,127,0.28);
        padding: 12px 13px;
        min-height: 118px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        background: var(--secondary-background-color);
    }
    .ops-card-top {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
    }
    .ops-card-title {
        font-size: 0.86rem;
        font-weight: 600;
        opacity: 0.86;
    }
    .ops-card-pill {
        border-radius: 999px;
        font-size: 0.72rem;
        font-weight: 700;
        padding: 2px 8px;
    }
    .ops-card-value {
        font-size: 1.86rem;
        font-weight: 700;
        line-height: 1;
        letter-spacing: 0.2px;
    }
    .ops-card-foot {
        font-size: 0.8rem;
        opacity: 0.84;
    }
    .ops-tone-good {
        border-color: rgba(22, 163, 74, 0.52);
        background: linear-gradient(180deg, rgba(22, 163, 74, 0.12), var(--secondary-background-color));
    }
    .ops-tone-good .ops-card-pill {
        background: rgba(22, 163, 74, 0.22);
        color: #86efac;
    }
    .ops-tone-danger {
        border-color: rgba(220, 38, 38, 0.52);
        background: linear-gradient(180deg, rgba(220, 38, 38, 0.12), var(--secondary-background-color));
    }
    .ops-tone-danger .ops-card-pill {
        background: rgba(220, 38, 38, 0.22);
        color: #fca5a5;
    }
    .ops-tone-neutral {
        border-color: rgba(148, 163, 184, 0.5);
        background: linear-gradient(180deg, rgba(100, 116, 139, 0.12), var(--secondary-background-color));
    }
    .ops-tone-neutral .ops-card-pill {
        background: rgba(100, 116, 139, 0.22);
        color: #cbd5e1;
    }
    [data-testid="stPlotlyChart"] {
        margin-bottom: 0.25rem;
    }
</style>
"""

CHART_HEIGHT = 300
MAX_HORIZONTAL_BARS = 8


def _to_date_range(periodo, fallback_start: dt.date, fallback_end: dt.date):
    if isinstance(periodo, (tuple, list)):
        if len(periodo) >= 2:
            start, end = periodo[0], periodo[1]
        elif len(periodo) == 1:
            start = end = periodo[0]
        else:
            start, end = fallback_start, fallback_end
    else:
        start = end = periodo

    if start is None:
        start = fallback_start
    if end is None:
        end = fallback_end
    if start > end:
        start, end = end, start
    return start, end


def _fmt_num(value, dec=1):
    value = float(value or 0.0)
    text = f"{value:,.{dec}f}"
    return text.replace(",", "X").replace(".", ",").replace("X", ".")


def _trend_label(snapshot: dict):
    trend = snapshot.get("trend", "stable")
    if trend == "up":
        return "Subindo"
    if trend == "down":
        return "Caindo"
    return "Estavel"


def _trend_symbol(snapshot: dict):
    trend = snapshot.get("trend", "stable")
    if trend == "up":
        return "↗"
    if trend == "down":
        return "↘"
    return "→"


def _render_card(container, title: str, formatted_value: str, snapshot: dict):
    tone = snapshot.get("tone", "neutral")
    tone_class = f"ops-tone-{escape(tone)}"
    delta = float(snapshot.get("delta", 0.0))
    delta_text = f"{delta:+.2f} vs periodo anterior"

    card_html = (
        f'<div class="ops-card {tone_class}">'
        f'<div class="ops-card-top"><div class="ops-card-title">{escape(title)}</div>'
        f'<span class="ops-card-pill">{escape(_trend_symbol(snapshot) + " " + _trend_label(snapshot))}</span></div>'
        f'<div class="ops-card-value">{escape(formatted_value)}</div>'
        f'<div class="ops-card-foot">{escape(delta_text)}</div>'
        "</div>"
    )
    container.markdown(card_html, unsafe_allow_html=True)


def _plot_line(df: pd.DataFrame, title: str, y_label: str, color: str):
    if df.empty:
        st.info("Sem dados para exibir.")
        return

    fig = px.line(df, x="data", y="valor", markers=False, color_discrete_sequence=[color])
    fig.update_layout(
        title=title,
        margin=dict(l=0, r=0, t=42, b=0),
        height=CHART_HEIGHT,
        xaxis_title=None,
        yaxis_title=y_label,
    )
    fig.update_xaxes(tickformat="%d/%m")
    st.plotly_chart(fig, use_container_width=True, theme="streamlit")


def _plot_bar(df: pd.DataFrame, title: str, x: str, y: str, y_label: str, color: str):
    if df.empty:
        st.info("Sem dados para exibir.")
        return

    fig = px.bar(df, x=x, y=y, color_discrete_sequence=[color])
    fig.update_layout(
        title=title,
        margin=dict(l=0, r=0, t=42, b=0),
        height=CHART_HEIGHT,
        xaxis_title=None,
        yaxis_title=y_label,
    )
    st.plotly_chart(fig, use_container_width=True, theme="streamlit")


def _plot_horizontal_bar(df: pd.DataFrame, title: str, y: str, x: str, x_label: str, color: str):
    if df.empty:
        st.info("Sem dados para exibir.")
        return

    chart_df = df.nlargest(MAX_HORIZONTAL_BARS, x).copy()
    chart_df[x] = pd.to_numeric(chart_df[x], errors="coerce").fillna(0.0)
    if chart_df[x].max() <= 0:
        st.info("Sem ocorrencias no periodo para este recorte (todos os valores estao em 0).")
        st.dataframe(
            chart_df[[y, x]].rename(columns={y: "Categoria", x: "Valor (%)"}),
            use_container_width=True,
            hide_index=True,
        )
        return

    fig = px.bar(chart_df, y=y, x=x, orientation="h", color_discrete_sequence=[color])
    fig.update_layout(
        title=title,
        margin=dict(l=0, r=0, t=42, b=0),
        height=CHART_HEIGHT,
        yaxis_title=None,
        xaxis_title=x_label,
        yaxis=dict(categoryorder="total ascending"),
    )
    st.plotly_chart(fig, use_container_width=True, theme="streamlit")


def render_performance_view(db):
    st.markdown(CARD_STYLE, unsafe_allow_html=True)
    st.markdown(
        """
<div class="ops-header">
    <h3 class="ops-title">Indicadores de Performance</h3>
    <p class="ops-subtitle">Visao de acuracidade, divergencia, giro, cobertura, ruptura e dead stock.</p>
</div>
""",
        unsafe_allow_html=True,
    )

    service = OperationalKPIsService(db)
    options = service.get_filter_options()
    min_date = options["date_min"]
    max_date = options["date_max"]

    default_start = max(min_date, max_date - dt.timedelta(days=90))

    fc1, fc2, fc3, fc4 = st.columns([2.0, 2.0, 1.2, 0.8], gap="medium")
    categorias = fc1.multiselect(
        "Categorias",
        options=options["categorias"],
        default=options["categorias"],
    )
    periodo = fc2.date_input(
        "Periodo",
        min_value=min_date,
        max_value=max_date,
        value=(default_start, max_date),
        key="perf_periodo",
    )
    dead_days = fc3.slider("Dead stock (dias)", min_value=30, max_value=180, value=60, step=5)
    if fc4.button("Atualizar", use_container_width=True):
        st.rerun()

    date_start, date_end = _to_date_range(periodo, default_start, max_date)

    data = service.get_performance_indicators(
        categorias=categorias,
        date_start=date_start,
        date_end=date_end,
        dead_days=dead_days,
    )

    cards = data["cards"]
    c1, c2, c3 = st.columns(3, gap="medium")
    _render_card(c1, "Acuracidade de Estoque", f"{_fmt_num(cards['acuracidade']['current'], 1)}%", cards["acuracidade"])
    _render_card(c2, "Divergencia de Inventario", f"{_fmt_num(cards['divergencia']['current'], 1)}%", cards["divergencia"])
    _render_card(c3, "Giro de Estoque", f"{_fmt_num(cards['giro']['current'], 2)}x", cards["giro"])

    c4, c5, c6 = st.columns(3, gap="medium")
    _render_card(c4, "Cobertura de Estoque", f"{_fmt_num(cards['cobertura']['current'], 1)} dias", cards["cobertura"])
    _render_card(c5, "Ruptura (Stockout Rate)", f"{_fmt_num(cards['ruptura']['current'], 1)}%", cards["ruptura"])
    _render_card(c6, "Dead Stock", f"{_fmt_num(cards['dead_stock']['current'], 1)}%", cards["dead_stock"])

    st.divider()

    s = data["series"]
    row1_l, row1_r = st.columns(2, gap="large")
    with row1_l:
        _plot_line(s["acuracidade"], "Acuracidade de Estoque - Tendencia", "Acuracidade (%)", "#22c55e")
    with row1_r:
        if not s["divergencia_categoria"].empty:
            _plot_horizontal_bar(
                s["divergencia_categoria"],
                "Divergencia de Inventario por Categoria",
                y="categoria",
                x="valor",
                x_label="Divergencia (%)",
                color="#f59e0b",
            )
        else:
            _plot_line(s["divergencia"], "Divergencia de Inventario - Tendencia", "Divergencia (%)", "#f59e0b")

    row2_l, row2_r = st.columns(2, gap="large")
    with row2_l:
        _plot_line(s["giro"], "Giro de Estoque - Tendencia", "Giro (x)", "#38bdf8")
    with row2_r:
        _plot_line(s["cobertura"], "Cobertura de Estoque - Tendencia", "Cobertura (dias)", "#818cf8")

    row3_l, row3_r = st.columns(2, gap="large")
    with row3_l:
        _plot_line(s["ruptura"], "Ruptura (Stockout Rate) - Tendencia", "Ruptura (%)", "#ef4444")
    with row3_r:
        if not s["dead_stock_categoria"].empty:
            _plot_horizontal_bar(
                s["dead_stock_categoria"],
                "Dead Stock por Categoria",
                y="categoria",
                x="valor",
                x_label="Dead stock (%)",
                color="#f97316",
            )
        else:
            _plot_bar(
                s["dead_stock_faixa"],
                "Distribuicao por Tempo sem Movimentacao",
                x="faixa",
                y="valor",
                y_label="Quantidade de SKUs",
                color="#f97316",
            )


def render_armazenagem_view(db):
    st.markdown(CARD_STYLE, unsafe_allow_html=True)
    st.markdown(
        """
<div class="ops-header">
    <h3 class="ops-title">Indicadores de Armazenagem</h3>
    <p class="ops-subtitle">Visao operacional de picking, erros de separacao, dock-to-stock e putaway.</p>
</div>
""",
        unsafe_allow_html=True,
    )

    service = OperationalKPIsService(db)
    options = service.get_filter_options()
    min_date = options["date_min"]
    max_date = options["date_max"]

    default_start = max(min_date, max_date - dt.timedelta(days=90))

    fc1, fc2, fc3, fc4 = st.columns([2.0, 2.0, 1.2, 0.8], gap="medium")
    categorias = fc1.multiselect(
        "Categorias",
        options=options["categorias"],
        default=options["categorias"],
        key="arm_categorias",
    )
    periodo = fc2.date_input(
        "Periodo",
        min_value=min_date,
        max_value=max_date,
        value=(default_start, max_date),
        key="arm_periodo",
    )
    shift_hours = fc3.slider("Horas por turno", min_value=4, max_value=12, value=8, step=1)
    if fc4.button("Atualizar", use_container_width=True, key="arm_refresh"):
        st.rerun()

    date_start, date_end = _to_date_range(periodo, default_start, max_date)

    data = service.get_armazenagem_indicators(
        categorias=categorias,
        date_start=date_start,
        date_end=date_end,
        shift_hours=shift_hours,
    )

    cards = data["cards"]
    c1, c2, c3, c4 = st.columns(4, gap="medium")
    _render_card(c1, "Produtividade de Separacao", f"{_fmt_num(cards['picking_rate']['current'], 2)} itens/h", cards["picking_rate"])
    _render_card(c2, "Erro de Separacao", f"{_fmt_num(cards['picking_error']['current'], 2)}%", cards["picking_error"])
    _render_card(c3, "Dock-to-Stock", f"{_fmt_num(cards['dock_to_stock']['current'], 2)} h", cards["dock_to_stock"])
    _render_card(c4, "Tempo de Putaway", f"{_fmt_num(cards['putaway']['current'], 2)} h", cards["putaway"])

    st.caption("Dock-to-Stock e Putaway estao modelados a partir do perfil de recebimento ate a integracao completa de eventos WMS.")

    st.divider()

    s = data["series"]
    row1_l, row1_r = st.columns(2, gap="large")
    with row1_l:
        _plot_line(
            s["picking_rate"],
            "Produtividade de Separacao (Picking Rate)",
            "Itens separados por hora",
            "#22c55e",
        )
    with row1_r:
        _plot_line(
            s["picking_error"],
            "Erro de Separacao (Picking Error Rate)",
            "Erro (%)",
            "#ef4444",
        )

    row2_l, row2_r = st.columns(2, gap="large")
    with row2_l:
        _plot_line(
            s["dock_to_stock"],
            "Dock-to-Stock (tempo medio)",
            "Tempo (h)",
            "#38bdf8",
        )
    with row2_r:
        _plot_line(
            s["putaway"],
            "Tempo de Putaway (tempo medio)",
            "Tempo (h)",
            "#a78bfa",
        )
