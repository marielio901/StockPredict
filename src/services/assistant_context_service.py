import datetime as dt
import re
import unicodedata
from typing import Any

import pandas as pd
from sqlalchemy.orm import Session

from src.models.schemas import Material, MovimentoEstoque
from src.repositories.alertas_repo import AlertasRepository
from src.repositories.estoque_repo import EstoqueRepository
from src.services.forecast_service import ForecastService
from src.services.kpis_service import KPIService


class AssistantContextService:
    PREDICTIVE_HINTS = {
        "previsao",
        "preditiva",
        "predicao",
        "futuro",
        "projecao",
        "horizonte",
        "cenario",
        "forecast",
        "demanda futura",
    }

    STOPWORDS = {
        "qual",
        "quais",
        "como",
        "esta",
        "estao",
        "sobre",
        "saldo",
        "item",
        "itens",
        "estoque",
        "dinheiro",
        "valor",
        "movimentacao",
        "movimentacoes",
        "mostrar",
        "me",
        "de",
        "do",
        "da",
        "dos",
        "das",
        "um",
        "uma",
        "os",
        "as",
        "no",
        "na",
        "por",
        "para",
        "com",
    }

    def __init__(self, db: Session):
        self.db = db
        self.kpi_service = KPIService(db)
        self.estoque_repo = EstoqueRepository(db)
        self.alertas_repo = AlertasRepository(db)
        self.forecast_service = ForecastService(db)

    @staticmethod
    def _normalize_text(value: Any) -> str:
        raw = str(value or "").strip().lower()
        norm = unicodedata.normalize("NFKD", raw)
        return "".join(ch for ch in norm if not unicodedata.combining(ch))

    @staticmethod
    def _fmt_num(value: Any, dec: int = 2) -> str:
        number = float(value or 0.0)
        text = f"{number:,.{dec}f}"
        return text.replace(",", "X").replace(".", ",").replace("X", ".")

    def _fmt_money(self, value: Any) -> str:
        return f"R${self._fmt_num(value, dec=2)}"

    @staticmethod
    def _fmt_date(value: Any) -> str:
        if value is None:
            return "-"
        if isinstance(value, pd.Timestamp):
            return value.strftime("%d/%m/%Y")
        if isinstance(value, dt.datetime):
            return value.strftime("%d/%m/%Y")
        if isinstance(value, dt.date):
            return value.strftime("%d/%m/%Y")
        return str(value)

    def should_include_predictive(self, question: str) -> bool:
        question_norm = self._normalize_text(question)
        return any(token in question_norm for token in self.PREDICTIVE_HINTS)

    def _load_estoque_df(self) -> pd.DataFrame:
        rows = self.estoque_repo.get_estoque_overview()
        if not rows:
            return pd.DataFrame(
                columns=[
                    "COD_ID",
                    "Descricao",
                    "Familia",
                    "Preco Medio (R$)",
                    "Estoque Atual",
                    "Entradas",
                    "Saidas",
                    "Estoque Minimo",
                    "Status",
                    "valor_em_estoque",
                ]
            )

        df = pd.DataFrame(rows)
        for col in [
            "Preco Medio (R$)",
            "Estoque Atual",
            "Entradas",
            "Saidas",
            "Estoque Minimo",
            "Saldo (%)",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        if "Status" not in df.columns:
            df["Status"] = "OK"

        df["valor_em_estoque"] = df["Estoque Atual"] * df["Preco Medio (R$)"]
        return df

    def _load_movimentos_df(self) -> pd.DataFrame:
        query = (
            self.db.query(
                MovimentoEstoque.data_mov,
                MovimentoEstoque.tipo,
                MovimentoEstoque.quantidade,
                MovimentoEstoque.custo_unit,
                MovimentoEstoque.documento_ref,
                Material.sku,
                Material.descricao,
                Material.categoria,
            )
            .join(Material, Material.id == MovimentoEstoque.material_id)
            .statement
        )

        df = pd.read_sql(query, self.db.bind)
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "data_mov",
                    "tipo",
                    "quantidade",
                    "custo_unit",
                    "documento_ref",
                    "sku",
                    "descricao",
                    "categoria",
                    "valor",
                ]
            )

        df["data_mov"] = pd.to_datetime(df["data_mov"], errors="coerce")
        df = df.dropna(subset=["data_mov"]).copy()
        df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0.0)
        df["custo_unit"] = pd.to_numeric(df["custo_unit"], errors="coerce").fillna(0.0)
        df["documento_ref"] = df["documento_ref"].fillna("").astype(str)
        df["valor"] = df["quantidade"] * df["custo_unit"]
        df["sku"] = df["sku"].fillna("")
        df["descricao"] = df["descricao"].fillna("")
        df["categoria"] = df["categoria"].fillna("Sem categoria")
        return df

    def _period_summary(self, df: pd.DataFrame, end_date: dt.date, days: int) -> dict:
        if df.empty:
            return {
                "days": int(days),
                "entradas_qtd": 0.0,
                "saidas_qtd": 0.0,
                "ajustes_qtd": 0.0,
                "saldo_liquido_qtd": 0.0,
                "entradas_valor": 0.0,
                "saidas_valor": 0.0,
            }

        start_date = end_date - dt.timedelta(days=max(1, int(days)) - 1)
        period = df[
            (df["data_mov"].dt.date >= start_date) & (df["data_mov"].dt.date <= end_date)
        ].copy()

        if period.empty:
            return {
                "days": int(days),
                "entradas_qtd": 0.0,
                "saidas_qtd": 0.0,
                "ajustes_qtd": 0.0,
                "saldo_liquido_qtd": 0.0,
                "entradas_valor": 0.0,
                "saidas_valor": 0.0,
            }

        entradas = period[period["tipo"] == "ENTRADA"]
        saidas = period[period["tipo"] == "SAIDA"]
        ajustes = period[period["tipo"] == "AJUSTE"]

        entradas_qtd = float(entradas["quantidade"].sum())
        saidas_qtd = float(saidas["quantidade"].sum())
        ajustes_qtd = float(ajustes["quantidade"].sum())

        return {
            "days": int(days),
            "entradas_qtd": entradas_qtd,
            "saidas_qtd": saidas_qtd,
            "ajustes_qtd": ajustes_qtd,
            "saldo_liquido_qtd": entradas_qtd - saidas_qtd + ajustes_qtd,
            "entradas_valor": float(entradas["valor"].sum()),
            "saidas_valor": float(saidas["valor"].sum()),
        }

    def _movement_timeline(self, df: pd.DataFrame, max_rows: int = 6) -> list[dict]:
        if df.empty:
            return []

        aux = df.copy()
        aux["periodo"] = aux["data_mov"].dt.to_period("M").astype(str)

        grouped = (
            aux.groupby(["periodo", "tipo"], as_index=False)
            .agg(qtd=("quantidade", "sum"))
            .sort_values(["periodo", "tipo"])
        )

        timeline = []
        for periodo in sorted(grouped["periodo"].unique())[-max_rows:]:
            chunk = grouped[grouped["periodo"] == periodo]
            entry = {
                "periodo": periodo,
                "entradas_qtd": float(chunk[chunk["tipo"] == "ENTRADA"]["qtd"].sum()),
                "saidas_qtd": float(chunk[chunk["tipo"] == "SAIDA"]["qtd"].sum()),
                "ajustes_qtd": float(chunk[chunk["tipo"] == "AJUSTE"]["qtd"].sum()),
            }
            timeline.append(entry)
        return timeline

    def _top_movement_items(self, df: pd.DataFrame, end_date: dt.date, days: int, tipo: str, limit: int = 5) -> list[dict]:
        if df.empty:
            return []

        start_date = end_date - dt.timedelta(days=max(1, int(days)) - 1)
        part = df[
            (df["data_mov"].dt.date >= start_date)
            & (df["data_mov"].dt.date <= end_date)
            & (df["tipo"] == tipo)
        ].copy()
        if part.empty:
            return []

        grouped = (
            part.groupby(["sku", "descricao", "categoria"], as_index=False)
            .agg(quantidade=("quantidade", "sum"), valor=("valor", "sum"))
            .sort_values("quantidade", ascending=False)
            .head(limit)
        )

        return [
            {
                "sku": str(row.sku),
                "descricao": str(row.descricao),
                "categoria": str(row.categoria),
                "quantidade": float(row.quantidade),
                "valor": float(row.valor),
            }
            for row in grouped.itertuples(index=False)
        ]

    def _latest_movement(self, df: pd.DataFrame, tipo: str, sku: str | None = None) -> dict | None:
        if df.empty:
            return None

        part = df[df["tipo"] == str(tipo).upper()].copy()
        if sku:
            sku_norm = self._normalize_text(sku)
            part = part[
                part["sku"].fillna("").astype(str).map(self._normalize_text) == sku_norm
            ].copy()

        if part.empty:
            return None

        row = part.sort_values("data_mov", ascending=False).iloc[0]
        return {
            "tipo": str(row.get("tipo", "")),
            "data": row.get("data_mov"),
            "sku": str(row.get("sku", "")),
            "descricao": str(row.get("descricao", "")),
            "categoria": str(row.get("categoria", "")),
            "quantidade": float(row.get("quantidade", 0.0)),
            "valor": float(row.get("valor", 0.0)),
            "documento_ref": str(row.get("documento_ref", "")),
        }

    def _extract_question_tokens(self, question: str) -> tuple[list[str], list[str]]:
        question_norm = self._normalize_text(question)
        sku_tokens = re.findall(r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+)+", str(question or "").upper())

        words = re.findall(r"[a-z0-9]{3,}", question_norm)
        terms = [w for w in words if w not in self.STOPWORDS]
        return sku_tokens, terms

    def _match_items(self, stock_df: pd.DataFrame, question: str, limit: int = 8) -> list[dict]:
        if stock_df.empty:
            return []

        sku_tokens, terms = self._extract_question_tokens(question)
        if not sku_tokens and not terms:
            return []

        work = stock_df.copy()
        work["_sku_norm"] = work["COD_ID"].fillna("").astype(str).map(self._normalize_text)
        work["_desc_norm"] = work["Descricao"].fillna("").astype(str).map(self._normalize_text)

        matched = pd.DataFrame(columns=work.columns)
        if sku_tokens:
            sku_norm_tokens = [self._normalize_text(token) for token in sku_tokens if str(token).strip()]
            exact_mask = pd.Series(False, index=work.index)
            for token_norm in sku_norm_tokens:
                exact_mask = exact_mask | (work["_sku_norm"] == token_norm)
            matched = work[exact_mask].copy()

            if matched.empty:
                contains_mask = pd.Series(False, index=work.index)
                for token_norm in sku_norm_tokens:
                    contains_mask = contains_mask | work["_sku_norm"].str.contains(
                        token_norm, na=False, regex=False
                    )
                matched = work[contains_mask].copy()

        if matched.empty and terms:
            text_mask = pd.Series(False, index=work.index)
            for term in terms:
                text_mask = text_mask | work["_sku_norm"].str.contains(term, na=False, regex=False)
                text_mask = text_mask | work["_desc_norm"].str.contains(term, na=False, regex=False)
            matched = work[text_mask].copy()

        if matched.empty:
            return []

        matched = matched.sort_values(["valor_em_estoque", "Estoque Atual"], ascending=[False, False])
        rows = []
        for _, row in matched.head(limit).iterrows():
            rows.append(
                {
                    "sku": str(row.get("COD_ID", "")),
                    "descricao": str(row.get("Descricao", "")),
                    "categoria": str(row.get("Familia", "")),
                    "saldo_qtd": float(row.get("Estoque Atual", 0.0)),
                    "saldo_valor": float(row.get("valor_em_estoque", 0.0)),
                    "status": str(row.get("Status", "OK")),
                    "preco_medio": float(row.get("Preco Medio (R$)", 0.0)),
                }
            )
        return rows

    def _top_inventory_items(self, stock_df: pd.DataFrame, limit: int = 8) -> list[dict]:
        if stock_df.empty:
            return []

        top = stock_df.sort_values("valor_em_estoque", ascending=False).head(limit)
        rows = []
        for _, row in top.iterrows():
            rows.append(
                {
                    "sku": str(row.get("COD_ID", "")),
                    "descricao": str(row.get("Descricao", "")),
                    "categoria": str(row.get("Familia", "")),
                    "saldo_qtd": float(row.get("Estoque Atual", 0.0)),
                    "saldo_valor": float(row.get("valor_em_estoque", 0.0)),
                    "status": str(row.get("Status", "OK")),
                }
            )
        return rows

    def _risk_item_lists(self, stock_df: pd.DataFrame, limit: int = 12) -> tuple[list[dict], list[dict]]:
        if stock_df.empty:
            return [], []

        work = stock_df.copy()
        work["Estoque Atual"] = pd.to_numeric(work["Estoque Atual"], errors="coerce").fillna(0.0)
        work["Estoque Minimo"] = pd.to_numeric(work["Estoque Minimo"], errors="coerce").fillna(0.0)
        work["Status"] = work["Status"].fillna("OK").astype(str)

        ruptura_df = work[
            (work["Status"].str.upper() == "RUPTURA") | (work["Estoque Atual"] <= 0)
        ].copy()
        ruptura_df = ruptura_df.sort_values(["Estoque Atual", "COD_ID"], ascending=[True, True]).head(limit)

        near_threshold = work["Estoque Minimo"].clip(lower=1.0) * 0.30
        near_df = work[
            (work["Estoque Atual"] > 0)
            & (
                work["Status"].str.upper().isin(["ABAIXO DO MINIMO", "ATENCAO"])
                | (work["Estoque Atual"] <= near_threshold)
            )
        ].copy()
        near_df["ratio_min"] = (
            near_df["Estoque Atual"] / near_df["Estoque Minimo"].replace(0, pd.NA)
        ).fillna(0.0)
        near_df = near_df.sort_values(["ratio_min", "COD_ID"], ascending=[True, True]).head(limit)

        def to_rows(df: pd.DataFrame) -> list[dict]:
            out = []
            for _, row in df.iterrows():
                out.append(
                    {
                        "sku": str(row.get("COD_ID", "")),
                        "descricao": str(row.get("Descricao", "")),
                        "categoria": str(row.get("Familia", "")),
                        "saldo_qtd": float(row.get("Estoque Atual", 0.0)),
                        "estoque_minimo": float(row.get("Estoque Minimo", 0.0)),
                        "saldo_valor": float(row.get("valor_em_estoque", 0.0)),
                        "status": str(row.get("Status", "OK")),
                    }
                )
            return out

        return to_rows(ruptura_df), to_rows(near_df)

    @staticmethod
    def _scenario_preview(df: pd.DataFrame, limit: int = 5) -> list[dict]:
        if df is None or df.empty:
            return []

        out = []
        for row in df.head(limit).itertuples(index=False):
            data = row._asdict()
            out.append(
                {
                    "sku": str(data.get("SKU", "")),
                    "descricao": str(data.get("Descricao", "")),
                    "familia": str(data.get("Familia", "")),
                    "estoque_atual": float(data.get("Estoque Atual", 0.0)),
                    "demanda_prevista": float(data.get("Demanda Prevista", 0.0)),
                    "estoque_projetado": float(data.get("Estoque Projetado", 0.0)),
                    "qtd_vencer": float(data.get("Qtd a Vencer", 0.0)),
                }
            )
        return out

    def _build_predictive_summary(self) -> dict | None:
        try:
            horizon_df, details = self.forecast_service.get_predictive_horizon_dashboard()
        except Exception:
            return None

        if horizon_df is None or horizon_df.empty:
            return {
                "horizontes": [],
                "top_ruptura_30": [],
                "top_vencer_30": [],
                "top_travados_30": [],
            }

        horizons = []
        for row in horizon_df.itertuples(index=False):
            horizons.append(
                {
                    "horizonte": str(getattr(row, "Horizonte", "")),
                    "dias": int(getattr(row, "Dias", 0) or 0),
                    "ruptura": int(getattr(row, "Ruptura", 0) or 0),
                    "vencer": int(getattr(row, "Vencer", 0) or 0),
                    "travados": int(getattr(row, "Travados", 0) or 0),
                    "suficientes": int(getattr(row, "Suficientes", 0) or 0),
                }
            )

        detail_30 = details.get(30, {}) if isinstance(details, dict) else {}
        return {
            "horizontes": horizons,
            "top_ruptura_30": self._scenario_preview(detail_30.get("ruptura")),
            "top_vencer_30": self._scenario_preview(detail_30.get("vencer")),
            "top_travados_30": self._scenario_preview(detail_30.get("travados")),
        }

    def build_context(self, question: str) -> dict:
        question = str(question or "").strip()
        stock_df = self._load_estoque_df()
        mov_df = self._load_movimentos_df()

        if mov_df.empty:
            reference_date = dt.date.today()
        else:
            reference_date = mov_df["data_mov"].max().date()

        main_kpis = self.kpi_service.get_main_kpis()

        try:
            min_date, max_date = self.kpi_service.get_date_bounds()
            global_summary = self.kpi_service.get_dashboard_summary(date_start=min_date, date_end=max_date)
        except Exception:
            min_date = reference_date - dt.timedelta(days=30)
            max_date = reference_date
            global_summary = {
                "val_entrada": 0.0,
                "val_saida": 0.0,
                "val_estoque": 0.0,
                "ruptura": 0,
                "abaixo_min": 0,
                "alertas": 0,
                "pendencias": 0,
            }

        saldo_total_qtd = float(stock_df["Estoque Atual"].sum()) if not stock_df.empty else 0.0
        saldo_total_valor = float(stock_df["valor_em_estoque"].sum()) if not stock_df.empty else 0.0
        ruptura_count = int((stock_df["Status"] == "RUPTURA").sum()) if not stock_df.empty else 0
        abaixo_min_count = (
            int((stock_df["Status"] == "ABAIXO DO MINIMO").sum()) if not stock_df.empty else 0
        )

        active_alerts = len(self.alertas_repo.list_alertas(resolvido=False))
        resolved_alerts = len(self.alertas_repo.list_alertas(resolvido=True))

        d7 = self._period_summary(mov_df, reference_date, 7)
        d30 = self._period_summary(mov_df, reference_date, 30)
        d90 = self._period_summary(mov_df, reference_date, 90)
        timeline = self._movement_timeline(mov_df, max_rows=6)

        top_saida_30 = self._top_movement_items(mov_df, reference_date, 30, "SAIDA", limit=5)
        top_entrada_30 = self._top_movement_items(mov_df, reference_date, 30, "ENTRADA", limit=5)
        latest_entrada = self._latest_movement(mov_df, "ENTRADA")
        latest_saida = self._latest_movement(mov_df, "SAIDA")

        matched_items = self._match_items(stock_df, question, limit=8)
        top_items = self._top_inventory_items(stock_df, limit=8)
        rupture_items, near_zero_items = self._risk_item_lists(stock_df, limit=12)
        latest_by_item = {}
        for item in matched_items[:5]:
            sku = str(item.get("sku", "")).strip()
            if not sku:
                continue
            latest_by_item[sku] = {
                "entrada": self._latest_movement(mov_df, "ENTRADA", sku=sku),
                "saida": self._latest_movement(mov_df, "SAIDA", sku=sku),
            }

        predictive = None
        if self.should_include_predictive(question):
            predictive = self._build_predictive_summary()

        return {
            "question": question,
            "reference_date": reference_date,
            "date_min": min_date,
            "date_max": max_date,
            "main_kpis": {
                "saldo": float(main_kpis.get("saldo", 0.0)),
                "entradas": float(main_kpis.get("entradas", 0.0)),
                "saidas": float(main_kpis.get("saidas", 0.0)),
                "cobertura_dias": float(main_kpis.get("cobertura_dias", 0.0)),
            },
            "finance": {
                "saldo_total_qtd": saldo_total_qtd,
                "saldo_total_valor": saldo_total_valor,
                "val_entrada_total": float(global_summary.get("val_entrada", 0.0)),
                "val_saida_total": float(global_summary.get("val_saida", 0.0)),
                "val_estoque_total": float(global_summary.get("val_estoque", saldo_total_valor)),
            },
            "inventory": {
                "item_count": int(len(stock_df)),
                "ruptura_count": ruptura_count,
                "abaixo_min_count": abaixo_min_count,
                "top_items": top_items,
                "matched_items": matched_items,
                "rupture_items": rupture_items,
                "near_zero_items": near_zero_items,
            },
            "alerts": {
                "ativos": int(active_alerts),
                "resolvidos": int(resolved_alerts),
            },
            "movements": {
                "d7": d7,
                "d30": d30,
                "d90": d90,
                "timeline": timeline,
                "top_saida_30": top_saida_30,
                "top_entrada_30": top_entrada_30,
                "latest": {
                    "entrada": latest_entrada,
                    "saida": latest_saida,
                    "by_item": latest_by_item,
                },
            },
            "predictive": predictive,
        }

    def to_prompt_context(self, context: dict) -> str:
        lines = []
        ref_date = context.get("reference_date", dt.date.today())
        lines.append(f"Data de referencia dos dados: {ref_date}")

        main_kpis = context.get("main_kpis", {})
        finance = context.get("finance", {})
        inventory = context.get("inventory", {})
        alerts = context.get("alerts", {})
        movements = context.get("movements", {})

        lines.append("\nResumo geral do estoque:")
        lines.append(f"- Saldo total de unidades: {self._fmt_num(main_kpis.get('saldo', 0.0), dec=2)}")
        lines.append(f"- Cobertura media: {self._fmt_num(main_kpis.get('cobertura_dias', 0.0), dec=1)} dias")
        lines.append(f"- Entradas acumuladas: {self._fmt_num(main_kpis.get('entradas', 0.0), dec=2)}")
        lines.append(f"- Saidas acumuladas: {self._fmt_num(main_kpis.get('saidas', 0.0), dec=2)}")

        lines.append("\nSaldo financeiro:")
        lines.append(f"- Valor total em estoque: {self._fmt_money(finance.get('saldo_total_valor', 0.0))}")
        lines.append(f"- Valor de entradas (periodo total): {self._fmt_money(finance.get('val_entrada_total', 0.0))}")
        lines.append(f"- Valor de saidas (periodo total): {self._fmt_money(finance.get('val_saida_total', 0.0))}")

        lines.append("\nSaude do inventario:")
        lines.append(f"- Itens monitorados: {int(inventory.get('item_count', 0))}")
        lines.append(f"- Itens em ruptura: {int(inventory.get('ruptura_count', 0))}")
        lines.append(f"- Itens abaixo do minimo: {int(inventory.get('abaixo_min_count', 0))}")
        lines.append(f"- Alertas ativos: {int(alerts.get('ativos', 0))}")

        rupture_items = inventory.get("rupture_items", [])
        if rupture_items:
            lines.append("- Principais itens em ruptura:")
            for item in rupture_items[:10]:
                lines.append(
                    "  * {sku} | {desc} | saldo {saldo} | min {minimo}".format(
                        sku=item.get("sku", ""),
                        desc=item.get("descricao", ""),
                        saldo=self._fmt_num(item.get("saldo_qtd", 0.0), dec=2),
                        minimo=self._fmt_num(item.get("estoque_minimo", 0.0), dec=2),
                    )
                )

        near_zero_items = inventory.get("near_zero_items", [])
        if near_zero_items:
            lines.append("- Principais itens perto de zerar:")
            for item in near_zero_items[:10]:
                lines.append(
                    "  * {sku} | {desc} | saldo {saldo} | min {minimo}".format(
                        sku=item.get("sku", ""),
                        desc=item.get("descricao", ""),
                        saldo=self._fmt_num(item.get("saldo_qtd", 0.0), dec=2),
                        minimo=self._fmt_num(item.get("estoque_minimo", 0.0), dec=2),
                    )
                )

        lines.append("\nMovimentacoes recentes:")
        for key in ["d7", "d30", "d90"]:
            item = movements.get(key, {})
            days = int(item.get("days", 0))
            lines.append(
                "- Ultimos {days} dias: entradas {ent} | saidas {sai} | ajustes {aju} | saldo liquido {liq}".format(
                    days=days,
                    ent=self._fmt_num(item.get("entradas_qtd", 0.0), dec=2),
                    sai=self._fmt_num(item.get("saidas_qtd", 0.0), dec=2),
                    aju=self._fmt_num(item.get("ajustes_qtd", 0.0), dec=2),
                    liq=self._fmt_num(item.get("saldo_liquido_qtd", 0.0), dec=2),
                )
            )

        latest = movements.get("latest", {})
        latest_entrada = latest.get("entrada")
        latest_saida = latest.get("saida")
        if latest_entrada:
            lines.append(
                "- Ultima entrada registrada: {data} | {sku} | qtd {qtd} | valor {valor}".format(
                    data=self._fmt_date(latest_entrada.get("data")),
                    sku=latest_entrada.get("sku", ""),
                    qtd=self._fmt_num(latest_entrada.get("quantidade", 0.0), dec=2),
                    valor=self._fmt_money(latest_entrada.get("valor", 0.0)),
                )
            )
        if latest_saida:
            lines.append(
                "- Ultima saida registrada: {data} | {sku} | qtd {qtd} | valor {valor}".format(
                    data=self._fmt_date(latest_saida.get("data")),
                    sku=latest_saida.get("sku", ""),
                    qtd=self._fmt_num(latest_saida.get("quantidade", 0.0), dec=2),
                    valor=self._fmt_money(latest_saida.get("valor", 0.0)),
                )
            )

        timeline = movements.get("timeline", [])
        if timeline:
            lines.append("- Evolucao mensal recente:")
            for row in timeline:
                lines.append(
                    "  * {periodo}: entradas {ent} | saidas {sai} | ajustes {aju}".format(
                        periodo=row.get("periodo", ""),
                        ent=self._fmt_num(row.get("entradas_qtd", 0.0), dec=1),
                        sai=self._fmt_num(row.get("saidas_qtd", 0.0), dec=1),
                        aju=self._fmt_num(row.get("ajustes_qtd", 0.0), dec=1),
                    )
                )

        matched_items = inventory.get("matched_items", [])
        if matched_items:
            lines.append("\nItens relacionados com a pergunta do usuario:")
            for item in matched_items[:8]:
                lines.append(
                    "- {sku} | {desc} | saldo {saldo} | valor {valor} | status {status}".format(
                        sku=item.get("sku", ""),
                        desc=item.get("descricao", ""),
                        saldo=self._fmt_num(item.get("saldo_qtd", 0.0), dec=2),
                        valor=self._fmt_money(item.get("saldo_valor", 0.0)),
                        status=item.get("status", "OK"),
                    )
                )
            latest_by_item = movements.get("latest", {}).get("by_item", {})
            if latest_by_item:
                lines.append("- Ultimas movimentacoes dos itens encontrados:")
                for sku, move in latest_by_item.items():
                    ent = move.get("entrada")
                    sai = move.get("saida")
                    ent_txt = (
                        f"entrada {self._fmt_date(ent.get('data'))} qtd {self._fmt_num(ent.get('quantidade', 0.0), 2)}"
                        if ent
                        else "entrada sem registro"
                    )
                    sai_txt = (
                        f"saida {self._fmt_date(sai.get('data'))} qtd {self._fmt_num(sai.get('quantidade', 0.0), 2)}"
                        if sai
                        else "saida sem registro"
                    )
                    lines.append(f"  * {sku}: {ent_txt} | {sai_txt}")
        else:
            top_items = inventory.get("top_items", [])
            if top_items:
                lines.append("\nTop itens por valor em estoque:")
                for item in top_items[:5]:
                    lines.append(
                        "- {sku} | {desc} | saldo {saldo} | valor {valor}".format(
                            sku=item.get("sku", ""),
                            desc=item.get("descricao", ""),
                            saldo=self._fmt_num(item.get("saldo_qtd", 0.0), dec=2),
                            valor=self._fmt_money(item.get("saldo_valor", 0.0)),
                        )
                    )

        predictive = context.get("predictive")
        if predictive is not None:
            lines.append("\nResumo preditivo de risco:")
            for row in predictive.get("horizontes", []):
                lines.append(
                    "- {h} ({d} dias): ruptura {r}, vencer {v}, travados {t}, suficientes {s}".format(
                        h=row.get("horizonte", ""),
                        d=int(row.get("dias", 0)),
                        r=int(row.get("ruptura", 0)),
                        v=int(row.get("vencer", 0)),
                        t=int(row.get("travados", 0)),
                        s=int(row.get("suficientes", 0)),
                    )
                )

            top_rupt = predictive.get("top_ruptura_30", [])
            if top_rupt:
                lines.append("- Principais itens com risco de ruptura no horizonte de 30 dias:")
                for item in top_rupt[:5]:
                    lines.append(
                        "  * {sku} | proj {proj} | demanda {dem}".format(
                            sku=item.get("sku", ""),
                            proj=self._fmt_num(item.get("estoque_projetado", 0.0), dec=2),
                            dem=self._fmt_num(item.get("demanda_prevista", 0.0), dec=2),
                        )
                    )

        return "\n".join(lines)
