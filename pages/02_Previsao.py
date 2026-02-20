from html import escape

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.db.engine import get_db
from src.models.schemas import Material
from src.services.forecast_service import ForecastService


def safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def clamp(value, low=0.0, high=100.0):
    return max(low, min(high, safe_float(value)))


def format_num_br(value, decimals=2):
    number = safe_float(value)
    txt = f"{number:,.{decimals}f}"
    return txt.replace(",", "X").replace(".", ",").replace("X", ".")


def format_money_br(value):
    return f"R${format_num_br(value, decimals=2)}"


def format_pct_br(value, decimals=1):
    return f"{format_num_br(value, decimals=decimals)}%"


def render_kpi_card(container, title, value, note, tone):
    tone_border = {
        "danger": "rgba(239, 68, 68, 0.55)",
        "warn": "rgba(245, 158, 11, 0.55)",
        "good": "rgba(34, 197, 94, 0.55)",
        "info": "rgba(56, 189, 248, 0.55)",
    }.get(tone, "rgba(127, 127, 127, 0.36)")
    tone_bg = {
        "danger": "rgba(239, 68, 68, 0.10)",
        "warn": "rgba(245, 158, 11, 0.10)",
        "good": "rgba(34, 197, 94, 0.10)",
        "info": "rgba(56, 189, 248, 0.10)",
    }.get(tone, "rgba(127, 127, 127, 0.08)")

    card_html = (
        f'<div class="pred-card" style="border-color:{tone_border}; '
        f'background:linear-gradient(180deg, {tone_bg} 0%, var(--secondary-background-color) 78%);">'
        f'<div class="pred-card-title">{escape(str(title))}</div>'
        f'<div class="pred-card-value">{escape(str(value))}</div>'
        f'<div class="pred-card-note">{escape(str(note))}</div>'
        "</div>"
    )
    container.markdown(card_html, unsafe_allow_html=True)


def _status_indicator(saldo_pct: float, qtd_vencer: float):
    saldo = clamp(saldo_pct)
    venc = safe_float(qtd_vencer)
    if saldo <= 0:
        return "RUPTURA"
    if venc > 0:
        return "VENCIMENTO"
    if saldo < 50:
        return "ATENCAO"
    if saldo < 70:
        return "ABAIXO MINIMO"
    return "OK"


def prepare_scenario_dataframe(df: pd.DataFrame):
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    number_cols = [
        "Estoque Atual",
        "Demanda Prevista",
        "Estoque Projetado",
        "Estoque Minimo",
        "Qtd a Vencer",
    ]
    for col in number_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)

    if "Saldo Projetado (%)" in out.columns:
        out["Saldo Projetado (%)"] = (
            pd.to_numeric(out["Saldo Projetado (%)"], errors="coerce")
            .fillna(0.0)
            .clip(lower=0.0, upper=100.0)
        )
    else:
        out["Saldo Projetado (%)"] = 0.0

    out["Status"] = out.apply(
        lambda r: _status_indicator(r.get("Saldo Projetado (%)", 0.0), r.get("Qtd a Vencer", 0.0)),
        axis=1,
    )

    for col in number_cols:
        if col in out.columns:
            out[col] = out[col].apply(lambda v: format_num_br(v, decimals=2))

    preferred_cols = [
        "SKU",
        "Descricao",
        "Familia",
        "Estoque Atual",
        "Demanda Prevista",
        "Estoque Projetado",
        "Estoque Minimo",
        "Saldo Projetado (%)",
        "Qtd a Vencer",
        "Status",
    ]
    visible_cols = [c for c in preferred_cols if c in out.columns]
    return out[visible_cols]


def render_scenario_table(df: pd.DataFrame, height=360):
    table_df = prepare_scenario_dataframe(df)
    if table_df.empty:
        return False

    column_cfg = {
        "Status": st.column_config.TextColumn(
            "Status",
            help="Indicador de risco do item no horizonte selecionado.",
        )
    }
    try:
        column_cfg["Saldo Projetado (%)"] = st.column_config.ProgressColumn(
            "Saldo Projetado (%)",
            min_value=0,
            max_value=100,
            format="%.1f%%",
            help="Relacao do estoque projetado frente ao estoque minimo.",
        )
    except Exception:
        column_cfg["Saldo Projetado (%)"] = st.column_config.NumberColumn(
            "Saldo Projetado (%)",
            format="%.1f%%",
        )

    st.dataframe(
        table_df,
        use_container_width=True,
        hide_index=True,
        height=height,
        column_config=column_cfg,
    )
    return True


def build_daily_projection(
    future_forecast: pd.DataFrame, estoque_atual: float, estoque_minimo: float
):
    if future_forecast is None or future_forecast.empty:
        return pd.DataFrame()

    sim = future_forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    sim["Consumo Base"] = pd.to_numeric(sim["yhat"], errors="coerce").fillna(0.0).clip(lower=0.0)
    sim["Consumo Otimista"] = (
        pd.to_numeric(sim["yhat_lower"], errors="coerce").fillna(0.0).clip(lower=0.0)
    )
    sim["Consumo Pessimista"] = (
        pd.to_numeric(sim["yhat_upper"], errors="coerce").fillna(0.0).clip(lower=0.0)
    )

    sim = sim.sort_values("ds").reset_index(drop=True)
    days = max(len(sim), 1)
    consumo_seguro_dia = max((safe_float(estoque_atual) - safe_float(estoque_minimo)) / days, 0.0)
    sim["Consumo Seguro"] = sim["Consumo Base"].clip(upper=consumo_seguro_dia)

    sim["Estoque Base"] = safe_float(estoque_atual) - sim["Consumo Base"].cumsum()
    sim["Estoque Otimista"] = safe_float(estoque_atual) - sim["Consumo Otimista"].cumsum()
    sim["Estoque Pessimista"] = safe_float(estoque_atual) - sim["Consumo Pessimista"].cumsum()
    sim["Estoque Seguro"] = safe_float(estoque_atual) - sim["Consumo Seguro"].cumsum()
    sim["Estoque Minimo"] = safe_float(estoque_minimo)
    return sim


def compute_risk_scores(snapshot: dict, sim: pd.DataFrame):
    estoque_min = max(safe_float(snapshot.get("estoque_minimo")), 1.0)
    estoque_atual = max(safe_float(snapshot.get("estoque_atual")), 1.0)
    qtd_a_vencer = max(safe_float(snapshot.get("qtd_a_vencer")), 0.0)
    intermitencia = clamp(snapshot.get("intermitencia", 1.0), 0.0, 1.0)
    buffer_seguranca = max(safe_float(snapshot.get("buffer_seguranca")), 0.0)

    if sim is None or sim.empty:
        raw_falta = 100.0 if snapshot.get("cenario_ruptura") else 0.0
        raw_perda = clamp((qtd_a_vencer / estoque_atual) * 100.0)
        if snapshot.get("cenario_perda"):
            raw_perda = max(raw_perda, 55.0)

        if raw_falta >= raw_perda:
            chance_falta = raw_falta
            chance_perda = min(raw_perda, max(0.0, 35.0 - (chance_falta * 0.25)))
        else:
            chance_perda = raw_perda
            chance_falta = min(raw_falta, max(0.0, 35.0 - (chance_perda * 0.25)))

        chance_suficiente = clamp(100.0 - max(chance_falta, chance_perda))
        return {
            "chance_falta": clamp(chance_falta),
            "chance_perda": clamp(chance_perda),
            "chance_suficiente": chance_suficiente,
        }

    pct_base_abaixo_min = float((sim["Estoque Base"] < estoque_min).mean() * 100.0)
    pct_pess_abaixo_min = float((sim["Estoque Pessimista"] < estoque_min).mean() * 100.0)
    pct_ruptura = float((sim["Estoque Pessimista"] <= 0).mean() * 100.0)
    raw_falta = clamp(max(pct_base_abaixo_min * 0.7 + pct_pess_abaixo_min * 0.6, pct_ruptura))
    raw_falta = clamp(raw_falta + ((1.0 - intermitencia) * 12.0))
    if snapshot.get("cenario_ruptura"):
        raw_falta = max(raw_falta, 70.0)

    excesso_seguro_final = max(float(sim["Estoque Seguro"].iloc[-1]) - estoque_min, 0.0)
    raw_perda = clamp(
        ((qtd_a_vencer / estoque_atual) * 100.0)
        + ((excesso_seguro_final / estoque_atual) * 45.0)
        + ((buffer_seguranca / estoque_atual) * 15.0)
    )
    if snapshot.get("cenario_perda"):
        raw_perda = max(raw_perda, 55.0)

    proj_base = safe_float(snapshot.get("estoque_proj_base"))
    shortage_regime = proj_base <= (estoque_min * 0.8)
    overstock_regime = proj_base >= (estoque_min * 1.8)

    if shortage_regime:
        chance_falta = raw_falta
        chance_perda = min(raw_perda, max(0.0, 32.0 - (chance_falta * 0.22)))
    elif overstock_regime:
        chance_perda = raw_perda
        chance_falta = min(raw_falta, max(0.0, 32.0 - (chance_perda * 0.22)))
    else:
        if raw_falta >= raw_perda:
            chance_falta = raw_falta
            chance_perda = min(raw_perda, max(0.0, 40.0 - (chance_falta * 0.25)))
        else:
            chance_perda = raw_perda
            chance_falta = min(raw_falta, max(0.0, 40.0 - (chance_perda * 0.25)))

    chance_falta = clamp(chance_falta)
    chance_perda = clamp(chance_perda)
    chance_suficiente = clamp(100.0 - max(chance_falta, chance_perda))

    return {
        "chance_falta": chance_falta,
        "chance_perda": chance_perda,
        "chance_suficiente": chance_suficiente,
    }


def compute_effectiveness_score(bt_df: pd.DataFrame):
    if bt_df is None or bt_df.empty:
        return None

    df = bt_df.copy()
    df["Total Real"] = pd.to_numeric(df.get("Total Real"), errors="coerce").fillna(0.0).clip(lower=0.0)
    df["Total Previsto"] = pd.to_numeric(df.get("Total Previsto"), errors="coerce").fillna(0.0).clip(lower=0.0)
    df["Informativa"] = (df["Total Real"] + df["Total Previsto"]) > 0
    df_info = df[df["Informativa"]].copy()
    if len(df_info) < 2:
        return None

    abs_err = (df_info["Total Previsto"] - df_info["Total Real"]).abs()
    denom_real = float(df_info["Total Real"].sum())

    if denom_real > 0:
        wmape = float((abs_err.sum() / denom_real) * 100.0)
    else:
        wmape = None

    smape = float(
        (
            200.0
            * abs_err
            / (df_info["Total Previsto"].abs() + df_info["Total Real"].abs() + 1e-9)
        ).mean()
    )

    score_smape = clamp(100.0 - (smape / 2.0))
    if wmape is None:
        score = score_smape
    else:
        score_wmape = clamp(100.0 - min(100.0, wmape))
        score = clamp((score_wmape * 0.65) + (score_smape * 0.35))

    return {
        "score": score,
        "wmape": wmape,
        "smape": smape,
        "windows": int(len(df_info)),
        "windows_total": int(len(df)),
    }


def build_trend_bars(
    hist_df: pd.DataFrame,
    fut_df: pd.DataFrame,
    hist_col: str,
    fut_col: str,
    title: str,
    color_hist: str,
    color_prev: str,
):
    hist_vals = pd.to_numeric(hist_df[hist_col], errors="coerce").fillna(0.0)
    fut_vals = pd.to_numeric(fut_df[fut_col], errors="coerce").fillna(0.0) if not fut_df.empty else pd.Series(dtype=float)
    all_vals = pd.concat([hist_vals, fut_vals], ignore_index=True)
    non_zero = all_vals[all_vals > 0]

    y_cap = 1.0
    clipped = False
    if not non_zero.empty:
        vmax = float(non_zero.max())
        p95 = float(non_zero.quantile(0.95))
        if vmax > (p95 * 1.7):
            y_cap = max(1.0, p95 * 1.15)
            clipped = True
        else:
            y_cap = max(1.0, vmax * 1.12)

    hist_plot = hist_vals.clip(upper=y_cap)
    fut_plot = fut_vals.clip(upper=y_cap) if not fut_df.empty else fut_vals
    hist_out = hist_vals > y_cap
    fut_out = fut_vals > y_cap if not fut_df.empty else pd.Series(dtype=bool)

    fig = go.Figure()
    fig.add_bar(
        x=hist_df["ds"],
        y=hist_plot,
        name="Historico",
        marker_color=color_hist,
    )
    if not fut_df.empty:
        fig.add_bar(
            x=fut_df["ds"],
            y=fut_plot,
            name="Previsto",
            marker_color=color_prev,
        )

    if hist_out.any():
        fig.add_scatter(
            x=hist_df.loc[hist_out, "ds"],
            y=[y_cap] * int(hist_out.sum()),
            mode="markers",
            name="Pico historico",
            marker=dict(symbol="triangle-up", size=8, color=color_hist),
            text=[f"Real: {format_num_br(v, 2)}" for v in hist_vals[hist_out]],
            hovertemplate="%{x|%d/%m/%Y}<br>%{text}<extra></extra>",
        )
    if not fut_df.empty and fut_out.any():
        fig.add_scatter(
            x=fut_df.loc[fut_out, "ds"],
            y=[y_cap] * int(fut_out.sum()),
            mode="markers",
            name="Pico previsto",
            marker=dict(symbol="diamond", size=7, color=color_prev),
            text=[f"Previsto: {format_num_br(v, 2)}" for v in fut_vals[fut_out]],
            hovertemplate="%{x|%d/%m/%Y}<br>%{text}<extra></extra>",
        )

    fig.update_layout(
        barmode="group",
        height=360,
        margin=dict(l=0, r=0, t=26, b=0),
        title=title,
        xaxis_title=None,
        yaxis_title="Qtd",
        bargap=0.10,
    )
    fig.update_xaxes(tickformat="%d/%m")
    fig.update_yaxes(range=[0, y_cap * 1.05])
    return fig, clipped


st.markdown(
    """
<style>
.pred-card {
    border: 1px solid rgba(127, 127, 127, 0.34);
    border-radius: 10px;
    padding: 12px 14px;
    min-height: 108px;
}
.pred-card-title {
    font-size: 0.92rem;
    font-weight: 600;
    opacity: 0.88;
}
.pred-card-value {
    margin-top: 4px;
    font-size: 2rem;
    font-weight: 800;
    line-height: 1.05;
}
.pred-card-note {
    margin-top: 6px;
    font-size: 0.83rem;
    opacity: 0.76;
}
.pred-block {
    border: 1px solid rgba(127, 127, 127, 0.24);
    border-radius: 10px;
    padding: 12px;
}
</style>
""",
    unsafe_allow_html=True,
)

db = next(get_db())
service = ForecastService(db)

st.title("Previsao de Demanda")
st.caption(
    "Visao automatica por horizonte e simulacao item a item sem painel pesado de parametros."
)

tab_geral, tab_simular = st.tabs(
    ["Analise Preditiva Geral", "Simular Cenários"]
)

with tab_geral:
    st.subheader("Dashboard Preditivo Geral")

    horizon_df, details = service.get_predictive_horizon_dashboard()
    if horizon_df.empty:
        st.warning("Nao existem dados suficientes para gerar cenarios gerais.")
    else:
        horizon_map = {
            "Proximo mes": 30,
            "Proximo semestre": 180,
            "Proximo ano": 365,
        }

        selected_horizon_label = st.radio(
            "Horizonte de acompanhamento",
            options=list(horizon_map.keys()),
            horizontal=True,
            index=0,
        )
        selected_days = horizon_map[selected_horizon_label]
        selected_details = details.get(selected_days, {})
        resumo = selected_details.get("resumo", {})

        c1, c2, c3, c4 = st.columns(4, gap="small")
        render_kpi_card(
            c1,
            "Itens que podem faltar",
            f"{int(resumo.get('ruptura', 0))}",
            f"Risco em ate {selected_days} dias",
            tone="danger",
        )
        render_kpi_card(
            c2,
            "Itens que podem vencer",
            f"{int(resumo.get('vencer', 0))}",
            f"Validade critica em ate {selected_days} dias",
            tone="warn",
        )
        render_kpi_card(
            c3,
            "Itens que podem travar",
            f"{int(resumo.get('travados', 0))}",
            "Excesso de estoque com giro baixo",
            tone="warn",
        )
        render_kpi_card(
            c4,
            "Itens suficientes",
            f"{int(resumo.get('suficientes', 0))}",
            "Faixa operacional adequada",
            tone="good",
        )

        chart_df = horizon_df.melt(
            id_vars=["Horizonte", "Dias"],
            value_vars=["Ruptura", "Vencer", "Travados", "Suficientes"],
            var_name="Cenario",
            value_name="Itens",
        )
        fig_horizon = px.bar(
            chart_df,
            x="Horizonte",
            y="Itens",
            color="Cenario",
            barmode="group",
            color_discrete_map={
                "Ruptura": "#FF1744",
                "Vencer": "#FFB300",
                "Travados": "#EF6C00",
                "Suficientes": "#22C55E",
            },
        )
        fig_horizon.update_layout(
            height=360,
            margin=dict(l=0, r=0, t=24, b=0),
            xaxis_title=None,
            yaxis_title="Quantidade de itens",
        )
        st.plotly_chart(fig_horizon, use_container_width=True, theme="streamlit")

        t1, t2, t3, t4 = st.tabs(
            [
                "Cenario de Ruptura",
                "Cenario de Vencimento",
                "Cenario de Travamento",
                "Cenario Suficiente",
            ]
        )

        with t1:
            if not render_scenario_table(selected_details.get("ruptura"), height=360):
                st.success("Sem itens com risco de ruptura neste horizonte.")

        with t2:
            if not render_scenario_table(selected_details.get("vencer"), height=360):
                st.success("Sem itens com risco de vencimento neste horizonte.")

        with t3:
            if not render_scenario_table(selected_details.get("travados"), height=360):
                st.success("Sem itens com risco de ficar travado neste horizonte.")

        with t4:
            if not render_scenario_table(selected_details.get("suficientes"), height=360):
                st.info("Sem itens classificados como suficientes neste horizonte.")

    st.markdown("### Tendencias gerais de entradas e saidas")
    trend_df = service.get_global_trends(history_days=180, forecast_days=30)
    if trend_df.empty:
        st.info("Sem dados para tendencia geral de movimentacao.")
    else:
        trend_df["ds"] = pd.to_datetime(trend_df["ds"], errors="coerce")
        trend_df = trend_df.dropna(subset=["ds"]).sort_values("ds")

        real_mask = (trend_df["Entradas"] > 0) | (trend_df["Saidas"] > 0)
        real_end = trend_df.loc[real_mask, "ds"].max() if real_mask.any() else trend_df["ds"].max()
        cutoff = real_end - pd.Timedelta(days=59)
        plot_df = trend_df[trend_df["ds"] >= cutoff].copy()
        hist_df = plot_df[plot_df["ds"] <= real_end].copy()
        fut_df = plot_df[plot_df["ds"] > real_end].copy()

        fig_ent, ent_clipped = build_trend_bars(
            hist_df=hist_df,
            fut_df=fut_df,
            hist_col="Entradas",
            fut_col="Entradas Prev",
            title="Entradas por dia",
            color_hist="#00E5FF",
            color_prev="rgba(0, 229, 255, 0.45)",
        )
        st.plotly_chart(fig_ent, use_container_width=True, theme="streamlit")
        if ent_clipped:
            st.caption("Entradas: picos extremos foram comprimidos e sinalizados para melhorar leitura.")

        fig_sai, sai_clipped = build_trend_bars(
            hist_df=hist_df,
            fut_df=fut_df,
            hist_col="Saidas",
            fut_col="Saidas Prev",
            title="Saidas por dia",
            color_hist="#FF1744",
            color_prev="rgba(255, 23, 68, 0.45)",
        )
        st.plotly_chart(fig_sai, use_container_width=True, theme="streamlit")
        if sai_clipped:
            st.caption("Saidas: picos extremos foram comprimidos e sinalizados para melhorar leitura.")

with tab_simular:
    st.subheader("Simular Cenarios Item a Item")

    materiais = (
        db.query(Material)
        .filter(Material.ativo == 1)
        .order_by(Material.sku.asc())
        .all()
    )
    if not materiais:
        st.warning("Nao existem itens ativos para simulacao.")
        st.stop()

    item_options = {
        f"{m.sku} - {m.descricao}": int(m.id)
        for m in materiais
    }

    sel_col, horizon_col = st.columns([2.8, 1.2])
    selected_label = sel_col.selectbox("Item", list(item_options.keys()))
    horizon_days = horizon_col.radio(
        "Horizonte",
        options=[30, 90, 180],
        index=0,
        horizontal=True,
    )
    material_id = item_options[selected_label]

    with st.spinner("Gerando simulacao preditiva..."):
        benchmark_df, benchmark_curves, benchmark_err = service.benchmark_models(
            material_id, horizon=min(30, int(horizon_days))
        )
        model_auto = service.default_model()
        if benchmark_err is None and not benchmark_df.empty:
            model_auto = str(benchmark_df.iloc[0]["Modelo"])

        forecast_df, forecast_err = service.train_forecast(
            material_id, periods=int(horizon_days), model_name=model_auto
        )
        history_df = service.get_historical_series(material_id)

    if forecast_err or forecast_df is None or history_df.empty:
        message = forecast_err or "Sem dados suficientes para previsao deste item."
        st.warning(message)
    else:
        history_df = history_df.copy()
        history_df["ds"] = pd.to_datetime(history_df["ds"], errors="coerce")
        history_df["y"] = pd.to_numeric(history_df["y"], errors="coerce").fillna(0.0)
        history_df = history_df.dropna(subset=["ds"]).sort_values("ds")

        forecast_df = forecast_df.copy()
        forecast_df["ds"] = pd.to_datetime(forecast_df["ds"], errors="coerce")
        last_hist_date = history_df["ds"].max()
        future_df = forecast_df[forecast_df["ds"] > last_hist_date].copy().sort_values("ds")
        future_df = future_df.head(int(horizon_days))

        st.markdown("### Previsao diaria do item selecionado")
        hist_cut = history_df.tail(90)
        fig_item = go.Figure()
        fig_item.add_bar(
            x=hist_cut["ds"],
            y=hist_cut["y"],
            name="Saida Real (dia)",
            marker_color="#00E5FF",
        )
        fig_item.add_bar(
            x=future_df["ds"],
            y=pd.to_numeric(future_df["yhat"], errors="coerce").fillna(0.0),
            name=f"Saida Prevista (dia) - {model_auto}",
            marker_color="#7EE787",
        )
        fig_item.update_layout(
            barmode="group",
            height=370,
            margin=dict(l=0, r=0, t=12, b=0),
            xaxis_title=None,
            yaxis_title="Quantidade por dia",
        )
        fig_item.update_xaxes(tickformat="%d/%m")
        st.plotly_chart(fig_item, use_container_width=True, theme="streamlit")

        snapshot = service.get_material_snapshot(
            material_id=material_id,
            periods=int(horizon_days),
            future_forecast=future_df,
        )
        sim_df = build_daily_projection(
            future_forecast=future_df,
            estoque_atual=safe_float(snapshot.get("estoque_atual") if snapshot else 0.0),
            estoque_minimo=safe_float(snapshot.get("estoque_minimo") if snapshot else 0.0),
        )
        risks = compute_risk_scores(snapshot or {}, sim_df)

        st.markdown("### Risco de falta, perdas e cobertura")
        r1, r2, r3, r4 = st.columns(4, gap="small")
        render_kpi_card(
            r1,
            "Chance de faltar",
            format_pct_br(risks["chance_falta"], decimals=1),
            "Probabilidade no horizonte simulado",
            tone="danger" if risks["chance_falta"] >= 50 else "warn",
        )
        render_kpi_card(
            r2,
            "Chance de perdas",
            format_pct_br(risks["chance_perda"], decimals=1),
            "Excesso + risco de vencimento",
            tone="warn" if risks["chance_perda"] >= 30 else "good",
        )
        render_kpi_card(
            r3,
            "Chance de suficiencia",
            format_pct_br(risks["chance_suficiente"], decimals=1),
            "Atender demanda sem ruptura",
            tone="good" if risks["chance_suficiente"] >= 70 else "warn",
        )
        render_kpi_card(
            r4,
            "Modelo aplicado",
            model_auto,
            "Escolha automatica por menor MAE",
            tone="info",
        )

        st.markdown("### Simulacao de serie temporal segura")
        if sim_df.empty:
            st.info("Nao foi possivel montar serie temporal segura para este item.")
        else:
            fig_stock = go.Figure()
            fig_stock.add_scatter(
                x=sim_df["ds"],
                y=sim_df["Estoque Base"],
                mode="lines",
                name="Estoque Base",
                line=dict(color="#93C5FD", width=2),
            )
            fig_stock.add_scatter(
                x=sim_df["ds"],
                y=sim_df["Estoque Pessimista"],
                mode="lines",
                name="Estoque Pessimista",
                line=dict(color="#F87171", width=2),
            )
            fig_stock.add_scatter(
                x=sim_df["ds"],
                y=sim_df["Estoque Seguro"],
                mode="lines",
                name="Serie Segura",
                line=dict(color="#22C55E", width=3),
            )
            fig_stock.add_scatter(
                x=sim_df["ds"],
                y=sim_df["Estoque Minimo"],
                mode="lines",
                name="Estoque Minimo",
                line=dict(color="#FBBF24", width=2, dash="dash"),
            )
            fig_stock.update_layout(
                height=380,
                margin=dict(l=0, r=0, t=14, b=0),
                xaxis_title=None,
                yaxis_title="Estoque projetado",
            )
            fig_stock.update_xaxes(tickformat="%d/%m")
            st.plotly_chart(fig_stock, use_container_width=True, theme="streamlit")

            stock_resume_cols = st.columns(4, gap="small")
            stock_resume_cols[0].metric(
                "Estoque atual",
                format_num_br(snapshot.get("estoque_atual"), decimals=2) if snapshot else "0,00",
            )
            stock_resume_cols[1].metric(
                "Estoque minimo",
                format_num_br(snapshot.get("estoque_minimo"), decimals=2) if snapshot else "0,00",
            )
            stock_resume_cols[2].metric(
                "Demanda prevista",
                format_num_br(snapshot.get("demanda_prevista"), decimals=2) if snapshot else "0,00",
            )
            stock_resume_cols[3].metric(
                "Qtd a vencer",
                format_num_br(snapshot.get("qtd_a_vencer"), decimals=2) if snapshot else "0,00",
            )

        st.markdown("### Benchmark de modelos e efetividade")
        if benchmark_err is not None or benchmark_df.empty:
            st.info(benchmark_err or "Benchmark indisponivel para este item.")
        else:
            bench_vis = benchmark_df.copy()
            mae_fig = px.bar(
                bench_vis.sort_values("MAE"),
                x="Modelo",
                y="MAE",
                color="Modelo",
                text_auto=".2f",
            )
            mae_fig.update_layout(
                showlegend=False,
                height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title=None,
                yaxis_title="MAE (quanto menor, melhor)",
            )
            st.plotly_chart(mae_fig, use_container_width=True, theme="streamlit")

            bt_df, bt_err = service.backtest_windows(
                material_id=material_id,
                model_name=model_auto,
                horizon=14,
                windows=4,
            )

            if bt_err is not None or bt_df.empty:
                st.info(bt_err or "Backtest indisponivel para este item.")
            else:
                bt_vis = bt_df.copy()
                eff = compute_effectiveness_score(bt_vis)
                if eff is None:
                    efetividade = None
                else:
                    efetividade = float(eff["score"])

                c_eff_1, c_eff_2 = st.columns([1, 2], gap="small")
                if efetividade is None:
                    render_kpi_card(
                        c_eff_1,
                        "Efetividade historica",
                        "N/A",
                        "Historico insuficiente para pontuacao confiavel",
                        tone="warn",
                    )
                else:
                    wmape_txt = (
                        "-"
                        if eff["wmape"] is None
                        else f"WMAPE {format_num_br(eff['wmape'], 1)}%"
                    )
                    note = (
                        f"{wmape_txt} | sMAPE {format_num_br(eff['smape'], 1)}% | "
                        f"{eff['windows']} de {eff['windows_total']} janelas informativas"
                    )
                    render_kpi_card(
                        c_eff_1,
                        "Efetividade historica",
                        format_pct_br(efetividade, decimals=1),
                        note,
                        tone="good" if efetividade >= 70 else "warn" if efetividade >= 45 else "danger",
                    )

                fig_bt = go.Figure()
                fig_bt.add_bar(
                    x=bt_vis["Janela"].astype(str),
                    y=bt_vis["Total Real"],
                    name="Total Real",
                    marker_color="#00E5FF",
                )
                fig_bt.add_bar(
                    x=bt_vis["Janela"].astype(str),
                    y=bt_vis["Total Previsto"],
                    name="Total Previsto",
                    marker_color="#7EE787",
                )
                fig_bt.update_layout(
                    barmode="group",
                    height=270,
                    margin=dict(l=0, r=0, t=4, b=0),
                    xaxis_title="Janela de teste",
                    yaxis_title="Volume no periodo",
                )
                c_eff_2.plotly_chart(fig_bt, use_container_width=True, theme="streamlit")

                bt_show = bt_vis.copy()
                for col in ["Total Real", "Total Previsto", "MAE", "RMSE", "Erro Total (%)", "WMAPE (%)"]:
                    if col in bt_show.columns:
                        bt_show[col] = bt_show[col].apply(
                            lambda v: format_num_br(v, decimals=2)
                        )
                st.dataframe(bt_show, use_container_width=True, hide_index=True, height=260)

            bench_show = bench_vis.copy()
            for col in ["MAE", "RMSE", "MAPE (%)", "WMAPE (%)", "Total Real", "Total Previsto", "Erro Total (%)"]:
                if col in bench_show.columns:
                    bench_show[col] = bench_show[col].apply(
                        lambda v: format_num_br(v, decimals=2) if pd.notna(v) else "-"
                    )
            st.dataframe(bench_show, use_container_width=True, hide_index=True, height=250)
