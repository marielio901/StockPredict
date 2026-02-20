from html import escape
import unicodedata

import pandas as pd
import streamlit as st

from src.db.engine import get_db
from src.repositories.estoque_repo import EstoqueRepository


def format_num_br(val, dec=2):
    num = float(val or 0.0)
    txt = f"{num:,.{dec}f}"
    return txt.replace(",", "X").replace(".", ",").replace("X", ".")


def format_money_br(val):
    return f"R${format_num_br(val, dec=2)}"


def normalize_text(value):
    text = str(value or "").strip().lower()
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch)
    )


def clamp_pct(val):
    return max(0.0, min(100.0, float(val or 0.0)))


def format_status(status):
    status = str(status or "").upper()
    if status == "RUPTURA":
        return "RUPTURA"
    if status == "ABAIXO DO MINIMO":
        return "ABAIXO DO MINIMO"
    if status == "ATENCAO":
        return "ATENCAO"
    return "OK"


def status_class(status):
    status = format_status(status)
    if status == "OK":
        return "status-ok"
    if status == "ABAIXO DO MINIMO":
        return "status-warn"
    return "status-danger"


def saldo_color(pct):
    if pct > 70:
        return "#16a34a"  # verde
    if pct > 30:
        return "#eab308"  # amarelo
    return "#dc2626"  # vermelho


def saldo_progress_html(pct):
    pct = clamp_pct(pct)
    color = saldo_color(pct)
    return (
        '<div class="saldo-wrap">'
        f'<div class="saldo-label">{format_num_br(pct, dec=1)}%</div>'
        '<div class="saldo-track">'
        f'<div class="saldo-fill" style="width:{pct:.1f}%; background:{color};"></div>'
        "</div>"
        "</div>"
    )


def render_estoque_table(df: pd.DataFrame, table_height: int):
    st.markdown(
        """
<style>
.estoque-wrap {
    overflow-y: auto;
    border: 1px solid rgba(127,127,127,0.22);
    border-radius: 10px;
}
.estoque-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.92rem;
}
.estoque-table thead th {
    position: sticky;
    top: 0;
    z-index: 1;
    background: rgba(20, 24, 36, 0.96);
    color: var(--text-color);
    border-bottom: 1px solid rgba(127,127,127,0.28);
    text-align: left;
    padding: 10px 8px;
    white-space: nowrap;
}
.estoque-table tbody td {
    border-bottom: 1px solid rgba(127,127,127,0.14);
    padding: 9px 8px;
    vertical-align: middle;
    white-space: nowrap;
}
.saldo-wrap {
    min-width: 138px;
}
.saldo-label {
    font-weight: 700;
    font-size: 0.82rem;
    margin-bottom: 4px;
}
.saldo-track {
    width: 100%;
    height: 10px;
    border-radius: 999px;
    background: rgba(255,255,255,0.10);
    overflow: hidden;
}
.saldo-fill {
    height: 100%;
    border-radius: 999px;
}
.status-badge {
    display: inline-block;
    border-radius: 999px;
    padding: 3px 10px;
    font-size: 0.78rem;
    font-weight: 700;
}
.status-ok {
    color: #86efac;
    background: rgba(22, 163, 74, 0.22);
}
.status-warn {
    color: #fcd34d;
    background: rgba(245, 158, 11, 0.22);
}
.status-danger {
    color: #fca5a5;
    background: rgba(220, 38, 38, 0.22);
}
</style>
""",
        unsafe_allow_html=True,
    )

    header_cells = [
        "COD_ID",
        "Descrição",
        "Familia",
        "Localização",
        "Unidade",
        "Preço Médio (R$)",
        "Entradas",
        "Saídas",
        "Estoque Atual",
        "Estoque Mínimo",
        "Saldo (%)",
        "Status",
    ]
    header_html = "".join(f"<th>{escape(col)}</th>" for col in header_cells)

    rows_html = []
    for _, row in df.iterrows():
        status = format_status(row["Status"])
        row_html = (
            "<tr>"
            f"<td>{escape(str(row['COD_ID']))}</td>"
            f"<td>{escape(str(row['Descricao']))}</td>"
            f"<td>{escape(str(row['Familia']))}</td>"
            f"<td>{escape(str(row['Localizacao']))}</td>"
            f"<td>{escape(str(row['Unidade']))}</td>"
            f"<td>{format_money_br(row['Preco Medio (R$)'])}</td>"
            f"<td>{format_num_br(row['Entradas'], dec=2)}</td>"
            f"<td>{format_num_br(row['Saidas'], dec=2)}</td>"
            f"<td>{format_num_br(row['Estoque Atual'], dec=2)}</td>"
            f"<td>{format_num_br(row['Estoque Minimo'], dec=2)}</td>"
            f"<td>{saldo_progress_html(row['Saldo (%)'])}</td>"
            f'<td><span class="status-badge {status_class(status)}">{escape(status)}</span></td>'
            "</tr>"
        )
        rows_html.append(row_html)

    table_html = (
        f'<div class="estoque-wrap" style="height:{table_height}px;">'
        '<table class="estoque-table">'
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{''.join(rows_html)}</tbody>"
        "</table>"
        "</div>"
    )
    st.markdown(table_html, unsafe_allow_html=True)


db = next(get_db())
repo = EstoqueRepository(db)

st.title("Estoque")
top_left, top_right = st.columns([3.1, 1.7])
with top_right:
    busca_estoque = st.text_input(
        "Pesquisar por COD_ID ou descricao",
        placeholder="Pesquisar COD_ID ou descricao",
        key="estoque_busca_cod_desc",
        label_visibility="collapsed",
    )

tab1, tab2, tab3 = st.tabs(["Visao de Estoque", "Novo Item", "Ajustar Estoque Minimo"])

with tab1:
    st.subheader("Posicao Atual de Estoque")
    overview = repo.get_estoque_overview()
    if overview:
        df = pd.DataFrame(overview)
        termo_busca = normalize_text(busca_estoque)
        if termo_busca:
            cod_series = df["COD_ID"].fillna("").astype(str).map(normalize_text)
            desc_series = df["Descricao"].fillna("").astype(str).map(normalize_text)
            mask = cod_series.str.contains(termo_busca, na=False, regex=False) | desc_series.str.contains(
                termo_busca, na=False, regex=False
            )
            df = df[mask].copy()
            st.caption(f"{len(df)} item(ns) encontrado(s) para '{busca_estoque.strip()}'.")
        max_rows = 15
        row_px = 36
        header_px = 44
        table_height = header_px + (max_rows * row_px)
        if df.empty:
            st.info("Nenhum item encontrado para o filtro informado.")
        else:
            render_estoque_table(df, table_height=table_height)
    else:
        st.info("Nenhum item encontrado no estoque.")

with tab2:
    st.subheader("Adicionar Novo Item")
    with st.form("form_material"):
        c1, c2 = st.columns(2)
        sku = c1.text_input("COD_ID")
        descricao = c2.text_input("Descricao")
        categorias_padrao = sorted(
            {
                "Herbicida",
                "Inseticida",
                "Fungicida",
                "Nematicida",
                "Fertilizante (NPK)",
                "Nitrogenado",
                "Fosfatado",
                "Potassico",
                "Micronutriente",
                "Inoculante",
                "Corretivo (Calcario)",
                "Organomineral",
                "Adjuvante",
                "Outros",
            }
        )
        categoria = c1.selectbox("Familia", categorias_padrao)
        unidade = c2.text_input("Unidade", "UN")
        minimo = c1.number_input("Estoque Minimo", min_value=0.0, value=10.0)
        lead_time = c2.number_input("Lead Time (Dias)", min_value=1, value=7)

        if st.form_submit_button("Salvar"):
            try:
                repo.create_material(
                    {
                        "sku": sku,
                        "descricao": descricao,
                        "categoria": categoria,
                        "unidade": unidade,
                        "estoque_minimo": minimo,
                        "lead_time_dias": lead_time,
                    }
                )
                st.success(f"Item {sku} criado com sucesso.")
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar: {e}")

with tab3:
    st.subheader("Ajustar Estoque Minimo")
    materiais = repo.list_materiais()
    if not materiais:
        st.info("Nenhum item cadastrado para ajuste.")
    else:
        item_options = {
            f"{m.sku} - {m.descricao}": m for m in sorted(materiais, key=lambda x: x.sku)
        }
        selected_label = st.selectbox("Item", list(item_options.keys()))
        selected = item_options[selected_label]

        minimo_atual = float(selected.estoque_minimo or 0.0)
        st.caption(f"Estoque minimo atual: {format_num_br(minimo_atual, dec=2)}")

        with st.form("form_ajuste_minimo"):
            novo_minimo = st.number_input(
                "Novo Estoque Minimo",
                min_value=0.0,
                value=max(minimo_atual, 10.0),
                step=1.0,
            )
            if st.form_submit_button("Salvar Ajuste"):
                try:
                    repo.update_material(selected.id, {"estoque_minimo": float(novo_minimo)})
                    st.success(f"Estoque minimo de {selected.sku} atualizado para {format_num_br(novo_minimo, dec=2)}.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao atualizar estoque minimo: {e}")
