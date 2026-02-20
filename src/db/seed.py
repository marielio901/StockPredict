import datetime as dt
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

import pandas as pd

from src.db.engine import Base, SessionLocal, engine
from src.models.schemas import Alerta, LocalEstoque, Lote, Material, MovimentoEstoque


def _normalize(text: str) -> str:
    base = unicodedata.normalize("NFKD", str(text))
    without_marks = "".join(ch for ch in base if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", without_marks.lower())


def _pick_column(df: pd.DataFrame, *candidates: str) -> str:
    normalized = {_normalize(col): col for col in df.columns}
    for candidate in candidates:
        key = _normalize(candidate)
        if key in normalized:
            return normalized[key]
    raise KeyError(f"Nao encontrei nenhuma coluna em {candidates}")


def _to_float(value, default: float = 0.0) -> float:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace("R$", "").replace(" ", "")
        if "," in cleaned and "." in cleaned:
            if cleaned.rfind(",") > cleaned.rfind("."):
                cleaned = cleaned.replace(".", "").replace(",", ".")
            else:
                cleaned = cleaned.replace(",", "")
        elif "," in cleaned:
            cleaned = cleaned.replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return default
    return default


def _to_date(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if isinstance(value, (int, float)):
        # Excel serial date (base 1899-12-30)
        return dt.date(1899, 12, 30) + dt.timedelta(days=int(value))
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _find_reference_workbook() -> Path | None:
    project_dir = Path(__file__).resolve().parents[2]  # stockpredict/
    scan_roots = [project_dir, project_dir.parent]

    for root in scan_roots:
        preferred = root / "Controle de Insumos Agricolas.xlsx"
        if preferred.exists():
            return preferred

    for root in scan_roots:
        matches = sorted(root.glob("Controle de Insumos*.xlsx"))
        if matches:
            return matches[0]

    return None


def _load_reference_data(workbook_path: Path):
    return {
        "itens": pd.read_excel(workbook_path, sheet_name="Itens"),
        "entradas": pd.read_excel(workbook_path, sheet_name="Entradas"),
        "saidas": pd.read_excel(workbook_path, sheet_name="Saidas"),
        "pedidos": pd.read_excel(workbook_path, sheet_name="Pedidos_Compra"),
        "estoque": pd.read_excel(workbook_path, sheet_name="Estoque"),
    }


def seed_db():
    print("[seed] Recriando banco...")
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    workbook_path = _find_reference_workbook()
    if not workbook_path:
        raise FileNotFoundError(
            "Planilha de referencia nao encontrada (Controle de Insumos*.xlsx)."
        )

    print(f"[seed] Usando planilha de referencia: {workbook_path}")
    sheets = _load_reference_data(workbook_path)

    itens_df = sheets["itens"].copy()
    entradas_df = sheets["entradas"].copy()
    saidas_df = sheets["saidas"].copy()
    pedidos_df = sheets["pedidos"].copy()
    estoque_df = sheets["estoque"].copy()

    # Flexible column mapping (tolerant to accents/spaces)
    it_sku_col = _pick_column(itens_df, "COD_ID", "Codigo")
    it_desc_col = _pick_column(itens_df, "Descricao", "Descrição")
    it_unit_col = _pick_column(itens_df, "Unidade")
    it_fam_col = _pick_column(itens_df, "Familia", "Família")
    it_price_col = _pick_column(itens_df, "Preco_Medio (R$)", "Preço_Médio (R$)", "Preco Medio (R$)")

    en_data_col = _pick_column(entradas_df, "Data")
    en_doc_col = _pick_column(entradas_df, "Documento")
    en_sku_col = _pick_column(entradas_df, "COD_ID")
    en_qty_col = _pick_column(entradas_df, "Quantidade")
    en_unit_col = _pick_column(entradas_df, "Preco Unitario (R$)", "Preço Unitário (R$)")

    sa_data_col = _pick_column(saidas_df, "Data")
    sa_doc_col = _pick_column(saidas_df, "Documento")
    sa_sku_col = _pick_column(saidas_df, "COD_ID")
    sa_qty_col = _pick_column(saidas_df, "Quantidade")
    sa_unit_col = _pick_column(saidas_df, "Preco Unitario (R$)", "Preço Unitário (R$)")

    es_sku_col = _pick_column(estoque_df, "COD_ID")
    es_loc_col = _pick_column(estoque_df, "Localizacao", "Localização")
    es_saldo_col = _pick_column(estoque_df, "Estoque Atual")

    pe_sku_col = _pick_column(pedidos_df, "COD_ID")
    pe_num_col = _pick_column(pedidos_df, "N Pedido", "Nº Pedido")
    pe_saldo_col = _pick_column(pedidos_df, "Saldo Aberto")
    pe_prev_col = _pick_column(pedidos_df, "Previsao Entrega", "Previsão Entrega")
    pe_status_col = _pick_column(pedidos_df, "Status")

    with SessionLocal() as db:
        # 1) Locais
        locais_nomes = ["Armazem Central"]
        for value in estoque_df[es_loc_col].dropna().tolist():
            nome = str(value).strip()
            if nome and nome not in locais_nomes:
                locais_nomes.append(nome)

        locais = [LocalEstoque(nome=nome) for nome in locais_nomes]
        db.add_all(locais)
        db.commit()

        locais_by_nome = {l.nome: l.id for l in db.query(LocalEstoque).all()}

        # 2) Materiais
        saida_totais = (
            saidas_df.groupby(sa_sku_col)[sa_qty_col].sum(min_count=1).fillna(0).to_dict()
        )
        estoque_atual = (
            estoque_df.set_index(es_sku_col)[es_saldo_col].apply(_to_float).to_dict()
        )

        materiais = []
        preco_medio_por_sku = {}
        local_por_sku = {}

        for _, row in itens_df.iterrows():
            sku = str(row[it_sku_col]).strip()
            if not sku or sku.lower() == "nan":
                continue

            descricao = str(row[it_desc_col]).strip()
            categoria = str(row[it_fam_col]).strip()
            unidade = str(row[it_unit_col]).strip()
            preco_medio = _to_float(row[it_price_col], default=0.0)

            saida_ref = _to_float(saida_totais.get(sku, 0.0))
            saldo_ref = _to_float(estoque_atual.get(sku, 0.0))
            estoque_minimo = max(round(max(saida_ref * 0.15, saldo_ref * 0.2), 2), 1.0)

            mat = Material(
                sku=sku,
                descricao=descricao,
                categoria=categoria,
                unidade=unidade,
                estoque_minimo=estoque_minimo,
                lead_time_dias=14,
            )
            materiais.append(mat)
            preco_medio_por_sku[sku] = preco_medio

        db.add_all(materiais)
        db.commit()

        sku_to_material = {m.sku: m for m in db.query(Material).all()}

        for _, row in estoque_df.iterrows():
            sku = str(row[es_sku_col]).strip()
            loc_name = str(row[es_loc_col]).strip()
            if sku in sku_to_material and loc_name in locais_by_nome:
                local_por_sku[sku] = locais_by_nome[loc_name]

        # 3) Movimentos
        timeline = []
        for _, row in entradas_df.iterrows():
            data_mov = _to_date(row[en_data_col])
            sku = str(row[en_sku_col]).strip()
            if not data_mov or sku not in sku_to_material:
                continue
            timeline.append(
                {
                    "tipo": "ENTRADA",
                    "data_mov": data_mov,
                    "sku": sku,
                    "quantidade": _to_float(row[en_qty_col]),
                    "custo_unit": _to_float(
                        row[en_unit_col], default=preco_medio_por_sku.get(sku, 0.0)
                    ),
                    "documento_ref": str(row[en_doc_col]).strip(),
                }
            )

        for _, row in saidas_df.iterrows():
            data_mov = _to_date(row[sa_data_col])
            sku = str(row[sa_sku_col]).strip()
            if not data_mov or sku not in sku_to_material:
                continue
            timeline.append(
                {
                    "tipo": "SAIDA",
                    "data_mov": data_mov,
                    "sku": sku,
                    "quantidade": _to_float(row[sa_qty_col]),
                    "custo_unit": _to_float(
                        row[sa_unit_col], default=preco_medio_por_sku.get(sku, 0.0)
                    ),
                    "documento_ref": str(row[sa_doc_col]).strip(),
                }
            )

        timeline.sort(
            key=lambda r: (r["data_mov"], 0 if r["tipo"] == "ENTRADA" else 1, r["sku"])
        )

        saldo_por_sku = defaultdict(float)
        ultima_entrada_por_sku = {}
        movimentos = []
        cortes_saida = 0

        for item in timeline:
            sku = item["sku"]
            quantidade = max(0.0, round(item["quantidade"], 2))
            if quantidade <= 0:
                continue

            material = sku_to_material[sku]
            local_id = local_por_sku.get(sku, locais_by_nome["Armazem Central"])
            custo_unit = max(0.0, round(item["custo_unit"], 4))

            if item["tipo"] == "ENTRADA":
                movimentos.append(
                    MovimentoEstoque(
                        material_id=material.id,
                        local_id=local_id,
                        tipo="ENTRADA",
                        quantidade=quantidade,
                        custo_unit=custo_unit,
                        data_mov=item["data_mov"],
                        documento_ref=item["documento_ref"] or "ENTRADA_PLANILHA",
                    )
                )
                saldo_por_sku[sku] += quantidade
                ultima_entrada_por_sku[sku] = item["data_mov"]
                continue

            # SAIDA: never exceeds available stock.
            saldo_disponivel = saldo_por_sku[sku]
            quantidade_validada = min(quantidade, saldo_disponivel)
            if quantidade_validada <= 0:
                continue
            if quantidade_validada < quantidade:
                cortes_saida += 1

            movimentos.append(
                MovimentoEstoque(
                    material_id=material.id,
                    local_id=local_id,
                    tipo="SAIDA",
                    quantidade=round(quantidade_validada, 2),
                    custo_unit=custo_unit,
                    data_mov=item["data_mov"],
                    documento_ref=item["documento_ref"] or "SAIDA_PLANILHA",
                )
            )
            saldo_por_sku[sku] -= quantidade_validada

        db.add_all(movimentos)
        db.commit()

        # 4) Lotes (populate the lot dashboard with real balances)
        hoje = dt.date.today()
        lotes = []
        for idx, material in enumerate(sorted(sku_to_material.values(), key=lambda m: m.sku)):
            saldo = round(saldo_por_sku.get(material.sku, 0.0), 2)
            if saldo <= 0:
                continue

            validade_offset = (idx * 17) % 360 - 30
            validade = hoje + dt.timedelta(days=validade_offset)
            if validade < hoje:
                status = "VENCIDO"
            elif validade <= hoje + dt.timedelta(days=30):
                status = "VENCENDO"
            else:
                status = "OK"

            lotes.append(
                Lote(
                    material_id=material.id,
                    local_id=local_por_sku.get(material.sku, locais_by_nome["Armazem Central"]),
                    lote=f"LT-{material.sku[-4:]}-{idx+1:03d}",
                    quantidade=saldo,
                    validade=validade,
                    data_entrada=ultima_entrada_por_sku.get(material.sku, hoje - dt.timedelta(days=15)),
                    status=status,
                )
            )

        db.add_all(lotes)
        db.commit()

        # 5) Alertas
        alertas = []
        for material in sku_to_material.values():
            saldo = round(saldo_por_sku.get(material.sku, 0.0), 2)
            local_id = local_por_sku.get(material.sku, locais_by_nome["Armazem Central"])

            if saldo <= 0:
                alertas.append(
                    Alerta(
                        material_id=material.id,
                        local_id=local_id,
                        tipo="RUPTURA",
                        severidade="ALTA",
                        mensagem=f"Ruptura no item {material.sku} ({material.descricao}).",
                        referencia_data=hoje,
                        resolvido=0,
                    )
                )
            elif saldo < material.estoque_minimo:
                alertas.append(
                    Alerta(
                        material_id=material.id,
                        local_id=local_id,
                        tipo="REPOSICAO",
                        severidade="MEDIA",
                        mensagem=(
                            f"Item {material.sku} abaixo do minimo: saldo {saldo:.2f} "
                            f"{material.unidade} / minimo {material.estoque_minimo:.2f}."
                        ),
                        referencia_data=hoje,
                        resolvido=0,
                    )
                )

        for _, row in pedidos_df.iterrows():
            sku = str(row[pe_sku_col]).strip()
            if sku not in sku_to_material:
                continue

            saldo_aberto = round(_to_float(row[pe_saldo_col]), 2)
            if saldo_aberto <= 0:
                continue

            material = sku_to_material[sku]
            status = str(row[pe_status_col]).strip()
            pedido = str(row[pe_num_col]).strip()
            previsao = _to_date(row[pe_prev_col])
            msg = (
                f"Pedido {pedido} ({status}) com saldo aberto de {saldo_aberto:.2f} "
                f"{material.unidade}. Previsao: {previsao or 'sem data'}."
            )

            alertas.append(
                Alerta(
                    material_id=material.id,
                    local_id=local_por_sku.get(material.sku, locais_by_nome["Armazem Central"]),
                    tipo="REPOSICAO",
                    severidade="BAIXA",
                    mensagem=msg,
                    referencia_data=hoje,
                    resolvido=0,
                )
            )

        db.add_all(alertas)
        db.commit()

        print(
            "[seed] Concluido: "
            f"{len(sku_to_material)} materiais, {len(movimentos)} movimentos, "
            f"{len(lotes)} lotes, {len(alertas)} alertas. "
            f"Saidas ajustadas: {cortes_saida}."
        )


if __name__ == "__main__":
    seed_db()
