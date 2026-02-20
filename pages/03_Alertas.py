import datetime as dt
from html import escape

import pandas as pd
import streamlit as st

from src.db.engine import get_db
from src.repositories.alertas_repo import AlertasRepository
from src.repositories.lotes_repo import LotesRepository
from src.services.kpis_service import KPIService
from src.utils.dates import get_vencimento_status

SEVERIDADE_RANK = {"ALTA": 3, "MEDIA": 2, "BAIXA": 1}
SEVERIDADE_CLASS = {"ALTA": "sev-high", "MEDIA": "sev-medium", "BAIXA": "sev-low"}
SEVERIDADE_LABEL = {"ALTA": "Alta", "MEDIA": "Media", "BAIXA": "Baixa"}

st.markdown(
    """
<style>
    html, body, [class*="css"] {
        font-family: "Bahnschrift", "Trebuchet MS", "Segoe UI", sans-serif;
    }
    .alerts-hero {
        border: 1px solid rgba(14, 116, 144, 0.38);
        border-radius: 14px;
        padding: 16px 18px;
        background:
            radial-gradient(circle at 18% 18%, rgba(34, 197, 94, 0.16), transparent 48%),
            radial-gradient(circle at 82% 20%, rgba(245, 158, 11, 0.14), transparent 52%),
            linear-gradient(130deg, rgba(14, 116, 144, 0.15), rgba(15, 23, 42, 0.12));
        margin-bottom: 0.75rem;
        animation: float-in 0.42s ease-out;
    }
    .hero-title {
        margin: 0;
        font-size: 1.34rem;
        font-weight: 700;
        letter-spacing: 0.2px;
    }
    .hero-text {
        margin: 0.32rem 0 0;
        opacity: 0.88;
        font-size: 0.95rem;
    }
    .signal-card {
        border-radius: 12px;
        border: 1px solid rgba(127, 127, 127, 0.28);
        padding: 12px 14px;
        min-height: 130px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        animation: float-in 0.5s ease-out;
    }
    .signal-top {
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .signal-icon {
        border-radius: 999px;
        min-width: 34px;
        height: 34px;
        padding: 0 10px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.3px;
    }
    .signal-title {
        font-size: 0.88rem;
        opacity: 0.86;
        font-weight: 600;
        line-height: 1.2;
    }
    .signal-value {
        font-size: 1.95rem;
        font-weight: 700;
        line-height: 1;
        letter-spacing: 0.4px;
    }
    .signal-foot {
        font-size: 0.82rem;
        opacity: 0.82;
    }
    .signal-neutral {
        background: linear-gradient(180deg, rgba(148, 163, 184, 0.12) 0%, var(--secondary-background-color) 100%);
    }
    .signal-good {
        border-color: rgba(34, 197, 94, 0.52);
        background: linear-gradient(180deg, rgba(34, 197, 94, 0.12) 0%, var(--secondary-background-color) 100%);
    }
    .signal-good .signal-icon {
        background: rgba(22, 163, 74, 0.25);
        color: #86efac;
    }
    .signal-warn {
        border-color: rgba(245, 158, 11, 0.56);
        background: linear-gradient(180deg, rgba(245, 158, 11, 0.14) 0%, var(--secondary-background-color) 100%);
    }
    .signal-warn .signal-icon {
        background: rgba(245, 158, 11, 0.27);
        color: #fcd34d;
    }
    .signal-danger {
        border-color: rgba(239, 68, 68, 0.56);
        background: linear-gradient(180deg, rgba(239, 68, 68, 0.14) 0%, var(--secondary-background-color) 100%);
    }
    .signal-danger .signal-icon {
        background: rgba(239, 68, 68, 0.28);
        color: #fca5a5;
    }
    .signal-neutral .signal-icon {
        background: rgba(148, 163, 184, 0.22);
        color: #cbd5e1;
    }
    .board-head {
        margin: 0 0 0.55rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .board-title {
        margin: 0;
        font-size: 1.08rem;
        font-weight: 700;
        letter-spacing: 0.1px;
    }
    .board-chip {
        border-radius: 999px;
        padding: 2px 10px;
        font-size: 0.74rem;
        font-weight: 700;
    }
    .board-chip-danger {
        background: rgba(239, 68, 68, 0.22);
        color: #fda4af;
    }
    .board-chip-warn {
        background: rgba(245, 158, 11, 0.2);
        color: #fcd34d;
    }
    .board-chip-good {
        background: rgba(34, 197, 94, 0.18);
        color: #86efac;
    }
    .risk-row {
        border: 1px solid rgba(127, 127, 127, 0.22);
        border-radius: 10px;
        padding: 10px 12px;
        margin-bottom: 9px;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 10px;
    }
    .risk-row-danger {
        border-color: rgba(239, 68, 68, 0.38);
        background: rgba(239, 68, 68, 0.08);
    }
    .risk-row-warn {
        border-color: rgba(245, 158, 11, 0.4);
        background: rgba(245, 158, 11, 0.09);
    }
    .risk-main {
        min-width: 0;
    }
    .risk-sku {
        font-size: 0.93rem;
        font-weight: 700;
        margin-bottom: 2px;
    }
    .risk-desc {
        font-size: 0.82rem;
        opacity: 0.82;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .risk-meta {
        text-align: right;
        font-size: 0.79rem;
        opacity: 0.88;
        line-height: 1.25;
    }
    .sev-pill {
        display: inline-block;
        border-radius: 999px;
        padding: 3px 10px;
        font-size: 0.76rem;
        font-weight: 700;
        letter-spacing: 0.2px;
    }
    .sev-high {
        color: #fecaca;
        background: rgba(239, 68, 68, 0.24);
    }
    .sev-medium {
        color: #fde68a;
        background: rgba(245, 158, 11, 0.22);
    }
    .sev-low {
        color: #bbf7d0;
        background: rgba(22, 163, 74, 0.24);
    }
    .alert-type-pill {
        display: inline-block;
        border-radius: 6px;
        padding: 3px 7px;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.15px;
        color: #bfdbfe;
        background: rgba(59, 130, 246, 0.22);
        border: 1px solid rgba(59, 130, 246, 0.35);
    }
    @keyframes float-in {
        0% {
            opacity: 0;
            transform: translateY(10px);
        }
        100% {
            opacity: 1;
            transform: translateY(0);
        }
    }
</style>
""",
    unsafe_allow_html=True,
)


def format_num_br(value: float, dec: int = 1) -> str:
    number = float(value or 0.0)
    text = f"{number:,.{dec}f}"
    return text.replace(",", "X").replace(".", ",").replace("X", ".")


def format_datetime_br(value, with_time: bool = False) -> str:
    if value is None:
        return "-"
    if isinstance(value, dt.datetime):
        return value.strftime("%d/%m/%Y %H:%M") if with_time else value.strftime("%d/%m/%Y")
    if isinstance(value, dt.date):
        return value.strftime("%d/%m/%Y")
    return str(value)


def severity_value(raw: str) -> str:
    return str(raw or "BAIXA").upper()


def severity_class(raw: str) -> str:
    return SEVERIDADE_CLASS.get(severity_value(raw), "sev-low")


def severity_label(raw: str) -> str:
    return SEVERIDADE_LABEL.get(severity_value(raw), "Baixa")


def sort_alertas(alertas):
    def key_func(alerta):
        sev = severity_value(getattr(alerta, "severidade", ""))
        created = getattr(alerta, "created_at", None) or dt.datetime.min
        return SEVERIDADE_RANK.get(sev, 0), created

    return sorted(alertas, key=key_func, reverse=True)


def material_label_from_alerta(alerta) -> str:
    material = getattr(alerta, "material", None)
    sku = getattr(material, "sku", "")
    descricao = getattr(material, "descricao", "")
    if str(sku).strip():
        if str(descricao).strip():
            return f"{sku} - {descricao}"
        return str(sku)
    material_id = getattr(alerta, "material_id", None)
    return f"Material #{material_id}" if material_id is not None else "Material nao informado"


def render_signal_card(container, title: str, value: str, foot: str, tone: str, icon: str):
    card_html = (
        f'<div class="signal-card signal-{escape(str(tone))}">'
        f'<div class="signal-top"><span class="signal-icon">{escape(icon)}</span><span class="signal-title">{escape(title)}</span></div>'
        f'<div class="signal-value">{escape(value)}</div>'
        f'<div class="signal-foot">{escape(foot)}</div>'
        "</div>"
    )
    container.markdown(card_html, unsafe_allow_html=True)


def render_board_header(title: str, total: int, chip_tone: str):
    chip_cls = f"board-chip board-chip-{chip_tone}"
    st.markdown(
        f'<div class="board-head"><h3 class="board-title">{escape(title)}</h3><span class="{chip_cls}">{int(total)} itens</span></div>',
        unsafe_allow_html=True,
    )


def render_risk_rows(df: pd.DataFrame, kind: str, max_rows: int = 8):
    if df.empty:
        st.info("Sem itens para exibir nesta faixa.")
        return

    tone_cls = "risk-row-danger" if kind == "danger" else "risk-row-warn"
    for _, row in df.head(max_rows).iterrows():
        st.markdown(
            (
                f'<div class="risk-row {tone_cls}">'
                f'<div class="risk-main"><div class="risk-sku">{escape(str(row["SKU"]))}</div>'
                f'<div class="risk-desc">{escape(str(row["Descricao"]))}</div></div>'
                f'<div class="risk-meta">{escape(str(row["Meta"]))}</div>'
                "</div>"
            ),
            unsafe_allow_html=True,
        )

    if len(df) > max_rows:
        st.caption(f"Mostrando os {max_rows} primeiros de {len(df)} itens.")


db = next(get_db())
repo = AlertasRepository(db)
lotes_repo = LotesRepository(db)
kpi_service = KPIService(db)

today = dt.date.today()
alertas_ativos = sort_alertas(repo.list_alertas(resolvido=False))
alertas_historico = sorted(
    repo.list_alertas(resolvido=True),
    key=lambda a: getattr(a, "resolved_at", None) or getattr(a, "created_at", None) or dt.datetime.min,
    reverse=True,
)

df_estoque = kpi_service.get_estoque_dataframe()
if not df_estoque.empty:
    for col in ["saldo", "estoque_minimo"]:
        df_estoque[col] = pd.to_numeric(df_estoque[col], errors="coerce").fillna(0.0)
else:
    df_estoque = pd.DataFrame(
        columns=["sku", "descricao", "saldo", "estoque_minimo", "lead_time_dias", "material_id"]
    )

rupturas = df_estoque[df_estoque["saldo"] <= 0].copy()
if not rupturas.empty:
    rupturas["deficit"] = (rupturas["estoque_minimo"] - rupturas["saldo"]).clip(lower=0)
    rupturas = rupturas.sort_values(["deficit", "sku"], ascending=[False, True])

abaixo_min = df_estoque[(df_estoque["saldo"] > 0) & (df_estoque["saldo"] < df_estoque["estoque_minimo"])].copy()
if not abaixo_min.empty:
    abaixo_min["gap"] = (abaixo_min["estoque_minimo"] - abaixo_min["saldo"]).clip(lower=0)
    abaixo_min = abaixo_min.sort_values(["gap", "sku"], ascending=[False, True])

lotes = lotes_repo.list_lotes()
lotes_rows = []
for lote in lotes:
    status = get_vencimento_status(lote.validade)
    if status not in {"VENCENDO", "VENCIDO"}:
        continue
    days_to_exp = None if lote.validade is None else (lote.validade - today).days
    material = getattr(lote, "material", None)
    sku = getattr(material, "sku", None) or f"Material #{lote.material_id}"
    descricao = getattr(material, "descricao", "") or "Sem descricao"
    lotes_rows.append(
        {
            "SKU": sku,
            "Descricao": descricao,
            "Lote": lote.lote,
            "Validade": lote.validade,
            "Quantidade": float(lote.quantidade or 0.0),
            "Status": status,
            "Dias": days_to_exp if days_to_exp is not None else 9999,
        }
    )

df_lotes_alerta = pd.DataFrame(lotes_rows)
if not df_lotes_alerta.empty:
    df_lotes_alerta = df_lotes_alerta.sort_values(["Status", "Dias", "SKU"], ascending=[False, True, True])

df_lotes_vencidos = (
    df_lotes_alerta[df_lotes_alerta["Status"] == "VENCIDO"].copy()
    if not df_lotes_alerta.empty
    else pd.DataFrame(columns=["SKU", "Descricao", "Lote", "Validade", "Quantidade", "Dias"])
)

df_lotes_vencendo = (
    df_lotes_alerta[df_lotes_alerta["Status"] == "VENCENDO"].copy()
    if not df_lotes_alerta.empty
    else pd.DataFrame(columns=["SKU", "Descricao", "Lote", "Validade", "Quantidade", "Dias"])
)

st.markdown(
    """
<div class="alerts-hero">
    <h2 class="hero-title">Central de Alertas Inteligente</h2>
    <p class="hero-text">
        Painel de risco para tratar rupturas, materiais abaixo do minimo e lotes com validade critica.
    </p>
</div>
""",
    unsafe_allow_html=True,
)

card_1, card_2, card_3, card_4, card_5 = st.columns(5, gap="medium")
alta_count = sum(1 for a in alertas_ativos if severity_value(getattr(a, "severidade", "")) == "ALTA")
tone_ativo = "danger" if len(alertas_ativos) > 0 else "good"
tone_alta = "danger" if alta_count > 0 else "good"
tone_rupt = "danger" if len(rupturas) > 0 else "good"
tone_venc = "warn" if len(df_lotes_vencendo) > 0 else "good"
tone_exp = "danger" if len(df_lotes_vencidos) > 0 else "good"

render_signal_card(card_1, "Pendencias Ativas", str(len(alertas_ativos)), "Fila de atendimento atual", tone_ativo, "ATV")
render_signal_card(card_2, "Prioridade Alta", str(alta_count), "Exigem tratativa imediata", tone_alta, "ALTA")
render_signal_card(card_3, "Itens em Ruptura", str(len(rupturas)), "Saldo zero ou negativo", tone_rupt, "RUP")
render_signal_card(card_4, "Lotes Vencendo", str(len(df_lotes_vencendo)), "Ate 30 dias para vencer", tone_venc, "30D")
render_signal_card(card_5, "Lotes Vencidos", str(len(df_lotes_vencidos)), "Bloquear e revisar uso", tone_exp, "VENC")

left_col, right_col = st.columns([1, 1], gap="large")

with left_col:
    render_board_header(
        "Quadro de Rupturas",
        len(rupturas),
        "danger" if len(rupturas) > 0 else "good",
    )
    if not rupturas.empty:
        ruptura_view = rupturas.copy()
        ruptura_view["SKU"] = ruptura_view["sku"]
        ruptura_view["Descricao"] = ruptura_view["descricao"].fillna("Sem descricao")
        ruptura_view["Meta"] = ruptura_view.apply(
            lambda r: (
                f"Saldo {format_num_br(r['saldo'], 1)} | Min {format_num_br(r['estoque_minimo'], 1)} | "
                f"Deficit {format_num_br(r['deficit'], 1)}"
            ),
            axis=1,
        )
        render_risk_rows(ruptura_view[["SKU", "Descricao", "Meta"]], kind="danger")
    else:
        st.info("Sem itens em ruptura no momento.")

    render_board_header(
        "Quadro Abaixo do Minimo",
        len(abaixo_min),
        "warn" if len(abaixo_min) > 0 else "good",
    )
    if not abaixo_min.empty:
        min_view = abaixo_min.copy()
        min_view["SKU"] = min_view["sku"]
        min_view["Descricao"] = min_view["descricao"].fillna("Sem descricao")
        min_view["Meta"] = min_view.apply(
            lambda r: (
                f"Saldo {format_num_br(r['saldo'], 1)} | Min {format_num_br(r['estoque_minimo'], 1)} | "
                f"Gap {format_num_br(r['gap'], 1)}"
            ),
            axis=1,
        )
        render_risk_rows(min_view[["SKU", "Descricao", "Meta"]], kind="warn")
    else:
        st.info("Sem itens abaixo do minimo.")

with right_col:
    tab_vencendo, tab_vencidos = st.tabs(["Vencendo em ate 30 dias", "Vencidos"])

    with tab_vencendo:
        render_board_header(
            "Lotes Proximos do Vencimento",
            len(df_lotes_vencendo),
            "warn" if len(df_lotes_vencendo) > 0 else "good",
        )
        if not df_lotes_vencendo.empty:
            vencendo_view = df_lotes_vencendo.copy()
            vencendo_view["Meta"] = vencendo_view.apply(
                lambda r: (
                    f"Lote {r['Lote']} | Val {format_datetime_br(r['Validade'])} | "
                    f"Qtd {format_num_br(r['Quantidade'], 1)} | D-{int(r['Dias'])}"
                ),
                axis=1,
            )
            render_risk_rows(vencendo_view[["SKU", "Descricao", "Meta"]], kind="warn")
        else:
            st.success("Nao ha lotes vencendo nos proximos 30 dias.")

    with tab_vencidos:
        render_board_header(
            "Lotes Vencidos",
            len(df_lotes_vencidos),
            "danger" if len(df_lotes_vencidos) > 0 else "good",
        )
        if not df_lotes_vencidos.empty:
            vencidos_view = df_lotes_vencidos.copy()
            vencidos_view["Meta"] = vencidos_view.apply(
                lambda r: (
                    f"Lote {r['Lote']} | Val {format_datetime_br(r['Validade'])} | "
                    f"Qtd {format_num_br(r['Quantidade'], 1)} | {abs(int(r['Dias']))} dias atrasado"
                ),
                axis=1,
            )
            render_risk_rows(vencidos_view[["SKU", "Descricao", "Meta"]], kind="danger")
        else:
            st.success("Sem lotes vencidos.")

st.divider()

ctrl_1, ctrl_2, ctrl_3 = st.columns([2.0, 1.4, 0.9], gap="medium")
tipos_disponiveis = sorted({str(a.tipo or "").upper() for a in alertas_ativos if str(a.tipo or "").strip()})
selected_tipos = ctrl_1.multiselect(
    "Filtrar por tipo de alerta",
    options=tipos_disponiveis,
    default=tipos_disponiveis,
)
selected_severidades = ctrl_2.multiselect(
    "Filtrar por severidade",
    options=["ALTA", "MEDIA", "BAIXA"],
    default=["ALTA", "MEDIA", "BAIXA"],
)
if ctrl_3.button("Atualizar painel", use_container_width=True):
    st.rerun()

st.subheader("Tratativa de Alertas Ativos")
filtered_alertas = [
    alerta
    for alerta in alertas_ativos
    if (not selected_tipos or str(alerta.tipo or "").upper() in selected_tipos)
    and (not selected_severidades or severity_value(getattr(alerta, "severidade", "")) in selected_severidades)
]

if not filtered_alertas:
    st.info("Nenhum alerta ativo encontrado para os filtros selecionados.")
else:
    for alerta in filtered_alertas:
        with st.container():
            info_col, sev_col, action_col = st.columns([5.4, 1.4, 1.2], gap="small")
            tipo = str(alerta.tipo or "ALERTA").upper()
            msg = str(alerta.mensagem or "Sem mensagem de detalhe.")
            created_text = format_datetime_br(getattr(alerta, "created_at", None), with_time=True)
            sev_raw = severity_value(getattr(alerta, "severidade", ""))

            info_col.markdown(f'<span class="alert-type-pill">{escape(tipo)}</span>', unsafe_allow_html=True)
            info_col.markdown(f"**{escape(material_label_from_alerta(alerta))}**")
            info_col.caption(msg)
            info_col.caption(f"Criado em: {created_text}")

            sev_col.markdown(
                f'<span class="sev-pill {severity_class(sev_raw)}">{escape(severity_label(sev_raw))}</span>',
                unsafe_allow_html=True,
            )

            if action_col.button("Resolver", key=f"resolve_alerta_{alerta.id}", use_container_width=True):
                repo.resolve_alerta(alerta.id)
                st.toast("Alerta marcado como resolvido.")
                st.rerun()

st.subheader("Historico de Alertas Resolvidos")
if alertas_historico:
    hist_rows = []
    for alerta in alertas_historico:
        hist_rows.append(
            {
                "Tipo": str(alerta.tipo or "").upper(),
                "Severidade": severity_label(alerta.severidade),
                "Material": material_label_from_alerta(alerta),
                "Criado em": format_datetime_br(alerta.created_at, with_time=True),
                "Resolvido em": format_datetime_br(alerta.resolved_at, with_time=True),
                "Mensagem": str(alerta.mensagem or ""),
            }
        )

    df_hist = pd.DataFrame(hist_rows)
    st.dataframe(
        df_hist,
        use_container_width=True,
        hide_index=True,
    )
else:
    st.caption("Ainda nao ha alertas resolvidos.")
