import datetime as dt

import pandas as pd
from sqlalchemy.orm import Session

from src.models.schemas import Alerta, Material, MovimentoEstoque


class KPIService:
    DASHBOARD_SERIES_END_DATE = dt.date(2026, 1, 10)
    DASHBOARD_SERIES_DAYS = 31

    def __init__(self, db: Session):
        self.db = db

    def list_categorias(self, categorias=None, skus=None):
        query = self.db.query(Material.categoria).filter(Material.categoria.isnot(None))

        categorias = categorias or []
        skus = skus or []
        if categorias:
            query = query.filter(Material.categoria.in_(categorias))
        if skus:
            query = query.filter(Material.sku.in_(skus))

        rows = query.distinct().all()
        categorias = sorted([r[0] for r in rows if str(r[0]).strip()])
        return categorias

    def list_itens(self, categorias=None):
        query = self.db.query(Material.sku, Material.descricao, Material.categoria)
        categorias = categorias or []
        if categorias:
            query = query.filter(Material.categoria.in_(categorias))

        rows = query.order_by(Material.sku.asc()).all()
        itens = [
            {"sku": r.sku, "descricao": r.descricao, "categoria": r.categoria}
            for r in rows
            if str(r.sku).strip()
        ]
        return itens

    def _default_series_window(self):
        series_end = self.DASHBOARD_SERIES_END_DATE
        series_start = series_end - dt.timedelta(days=self.DASHBOARD_SERIES_DAYS - 1)
        return series_start, series_end

    def _normalize_date_range(self, date_start=None, date_end=None):
        if date_start is None and date_end is None:
            return self._default_series_window()
        if date_start is None:
            date_start = date_end
        if date_end is None:
            date_end = date_start
        if date_start > date_end:
            date_start, date_end = date_end, date_start
        return date_start, date_end

    def _apply_filters(self, df, categorias=None, skus=None, date_start=None, date_end=None):
        categorias = categorias or []
        skus = skus or []

        filtered = df.copy()
        if categorias:
            filtered = filtered[filtered["categoria"].isin(categorias)]
        if skus:
            filtered = filtered[filtered["sku"].isin(skus)]
        if date_start:
            filtered = filtered[filtered["data_mov"].dt.date >= date_start]
        if date_end:
            filtered = filtered[filtered["data_mov"].dt.date <= date_end]
        return filtered

    def get_date_bounds(self, categorias=None, skus=None):
        df = self._movimentos_dataframe()
        if df.empty:
            return self._default_series_window()

        filtered = self._apply_filters(df, categorias=categorias, skus=skus)
        if filtered.empty:
            return self._default_series_window()

        return filtered["data_mov"].min().date(), filtered["data_mov"].max().date()

    def _empty_summary(self, date_start=None, date_end=None, categorias=None, skus=None):
        series_start, series_end = self._normalize_date_range(date_start, date_end)
        window_idx = pd.date_range(series_start, series_end, freq="D")

        zeros = pd.Series(0.0, index=window_idx)
        serie_entradas = zeros.reset_index()
        serie_entradas.columns = ["data", "valor"]
        serie_saidas = zeros.reset_index()
        serie_saidas.columns = ["data", "valor"]

        categorias_base = self.list_categorias(categorias=categorias, skus=skus)
        bar_familia = pd.DataFrame(
            {"categoria": categorias_base, "valor": [0.0] * len(categorias_base)}
        )

        return {
            "ruptura": 0,
            "abaixo_min": 0,
            "cobertura": 0.0,
            "alertas": 0,
            "val_entrada": 0.0,
            "val_saida": 0.0,
            "val_estoque": 0.0,
            "pendencias": 0,
            "series_entradas": serie_entradas,
            "series_saidas": serie_saidas,
            "series_start": series_start,
            "series_end": series_end,
            "bar_familia": bar_familia,
        }

    def _smooth_series(self, raw_series: pd.Series) -> pd.Series:
        """
        Suaviza picos distribuindo volumes para dias vizinhos sem alterar o total do periodo.
        """
        raw_series = pd.to_numeric(raw_series, errors="coerce").fillna(0.0)
        # Suavizacao forte em duas etapas para evitar qualquer pico agressivo.
        smoothed = raw_series.rolling(window=21, center=True, min_periods=1).mean()
        smoothed = smoothed.rolling(window=7, center=True, min_periods=1).mean()

        raw_total = float(raw_series.sum())
        smooth_total = float(smoothed.sum())
        if raw_total > 0 and smooth_total > 0:
            smoothed = smoothed * (raw_total / smooth_total)

        return smoothed.clip(lower=0.0)

    def _movimentos_dataframe(self) -> pd.DataFrame:
        query = (
            self.db.query(
                MovimentoEstoque.material_id,
                MovimentoEstoque.tipo,
                MovimentoEstoque.quantidade,
                MovimentoEstoque.custo_unit,
                MovimentoEstoque.data_mov,
                Material.sku,
                Material.descricao,
                Material.categoria,
                Material.unidade,
                Material.estoque_minimo,
                Material.lead_time_dias,
            )
            .join(Material, Material.id == MovimentoEstoque.material_id)
            .statement
        )

        df = pd.read_sql(query, self.db.bind)
        if df.empty:
            return df

        df["data_mov"] = pd.to_datetime(df["data_mov"], errors="coerce")
        df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0.0)
        df["custo_unit"] = pd.to_numeric(df["custo_unit"], errors="coerce").fillna(0.0)
        df["valor"] = df["quantidade"] * df["custo_unit"]

        def signed_qty(row):
            if row["tipo"] == "SAIDA":
                return -row["quantidade"]
            return row["quantidade"]

        df["qtd_ajustada"] = df.apply(signed_qty, axis=1)
        return df

    def get_dashboard_summary(self, categorias=None, skus=None, date_start=None, date_end=None):
        df = self._movimentos_dataframe()
        if df.empty:
            return self._empty_summary(
                date_start,
                date_end,
                categorias=categorias,
                skus=skus,
            )

        date_start, date_end = self._normalize_date_range(date_start, date_end)
        df = self._apply_filters(
            df,
            categorias=categorias,
            skus=skus,
            date_start=date_start,
            date_end=date_end,
        )
        if df.empty:
            return self._empty_summary(
                date_start,
                date_end,
                categorias=categorias,
                skus=skus,
            )

        estoque = (
            df.groupby(
                [
                    "material_id",
                    "sku",
                    "descricao",
                    "categoria",
                    "unidade",
                    "estoque_minimo",
                    "lead_time_dias",
                ],
                as_index=False,
            )
            .agg(saldo=("qtd_ajustada", "sum"))
        )

        entradas = df[df["tipo"] == "ENTRADA"].copy()
        saidas = df[df["tipo"] == "SAIDA"].copy()

        if entradas.empty:
            custo_medio = estoque[["material_id"]].copy()
            custo_medio["custo_medio"] = 0.0
        else:
            custo_medio = (
                entradas.groupby("material_id", as_index=False)
                .agg(total_qty=("quantidade", "sum"), total_valor=("valor", "sum"))
            )
            custo_medio["custo_medio"] = custo_medio.apply(
                lambda row: row["total_valor"] / row["total_qty"]
                if row["total_qty"] > 0
                else 0.0,
                axis=1,
            )
            custo_medio = custo_medio[["material_id", "custo_medio"]]
        estoque = estoque.merge(custo_medio, on="material_id", how="left")
        estoque["custo_medio"] = estoque["custo_medio"].fillna(0.0)

        ruptura_count = int((estoque["saldo"] <= 0).sum())
        abaixo_min_count = int((estoque["saldo"] < estoque["estoque_minimo"]).sum())

        end_date = date_end
        start_date_90 = end_date - dt.timedelta(days=90)
        total_saida_90 = float(
            saidas[saidas["data_mov"].dt.date >= start_date_90]["quantidade"].sum()
        )
        consumo_diario_medio = total_saida_90 / 90 if total_saida_90 > 0 else 0.0

        saldo_total_geral = float(estoque["saldo"].sum())
        cobertura_media_dias = (
            saldo_total_geral / consumo_diario_medio if consumo_diario_medio > 0 else 0.0
        )

        material_ids = [int(m) for m in estoque["material_id"].unique().tolist()]
        alertas_query = self.db.query(Alerta).filter(Alerta.resolvido == 0)
        if categorias or skus:
            if material_ids:
                alertas_query = alertas_query.filter(Alerta.material_id.in_(material_ids))
            else:
                alertas_query = alertas_query.filter(Alerta.material_id == -1)
        alertas_query = alertas_query.filter(
            Alerta.referencia_data >= date_start, Alerta.referencia_data <= date_end
        )
        alertas_count = alertas_query.count()

        total_entrada = float(entradas["valor"].sum())
        total_saida = float(saidas["valor"].sum())
        estoque["saldo_positivo"] = estoque["saldo"].clip(lower=0)
        val_estoque = float((estoque["saldo_positivo"] * estoque["custo_medio"]).sum())

        pendencias_query = self.db.query(Alerta).filter(
            Alerta.resolvido == 0, Alerta.severidade == "ALTA"
        )
        if categorias or skus:
            if material_ids:
                pendencias_query = pendencias_query.filter(
                    Alerta.material_id.in_(material_ids)
                )
            else:
                pendencias_query = pendencias_query.filter(Alerta.material_id == -1)
        pendencias_query = pendencias_query.filter(
            Alerta.referencia_data >= date_start, Alerta.referencia_data <= date_end
        )
        pendencias = pendencias_query.count()

        series_start, series_end = date_start, date_end
        window_idx = pd.date_range(series_start, series_end, freq="D")

        serie_entradas_raw = (
            df[df["tipo"].isin(["ENTRADA", "AJUSTE"])]
            .groupby("data_mov")["valor"]
            .sum()
            .reindex(window_idx, fill_value=0)
        )
        serie_entradas = self._smooth_series(serie_entradas_raw).reset_index()
        serie_entradas.columns = ["data", "valor"]

        serie_saidas_raw = (
            saidas.groupby("data_mov")["valor"]
            .sum()
            .reindex(window_idx, fill_value=0)
        )
        serie_saidas = self._smooth_series(serie_saidas_raw).reset_index()
        serie_saidas.columns = ["data", "valor"]

        categorias_base = self.list_categorias(categorias=categorias, skus=skus)
        consumo_por_categoria = (
            saidas.groupby("categoria", as_index=False)["valor"].sum()
            if not saidas.empty
            else pd.DataFrame(columns=["categoria", "valor"])
        )
        bar_familia = pd.DataFrame({"categoria": categorias_base}).merge(
            consumo_por_categoria,
            on="categoria",
            how="left",
        )
        bar_familia["valor"] = (
            pd.to_numeric(bar_familia["valor"], errors="coerce")
            .fillna(0.0)
            .astype(float)
        )
        bar_familia = bar_familia.sort_values(
            ["valor", "categoria"], ascending=[True, True]
        )

        return {
            "ruptura": ruptura_count,
            "abaixo_min": abaixo_min_count,
            "cobertura": round(float(cobertura_media_dias), 1),
            "alertas": int(alertas_count),
            "val_entrada": total_entrada,
            "val_saida": total_saida,
            "val_estoque": val_estoque,
            "pendencias": int(pendencias),
            "series_entradas": serie_entradas,
            "series_saidas": serie_saidas,
            "series_start": series_start,
            "series_end": series_end,
            "bar_familia": bar_familia,
        }

    def get_main_kpis(self):
        df = self._movimentos_dataframe()
        if df.empty:
            return {
                "saldo": 0.0,
                "entradas": 0.0,
                "saidas": 0.0,
                "cobertura_dias": 0.0,
            }

        entradas = float(df[df["tipo"] == "ENTRADA"]["quantidade"].sum())
        saidas = float(df[df["tipo"] == "SAIDA"]["quantidade"].sum())
        ajustes = float(df[df["tipo"] == "AJUSTE"]["quantidade"].sum())
        saldo = entradas - saidas + ajustes

        end_date = df["data_mov"].max().date()
        start_date_90 = end_date - dt.timedelta(days=90)
        saidas_90 = float(
            df[
                (df["tipo"] == "SAIDA")
                & (df["data_mov"].dt.date >= start_date_90)
            ]["quantidade"].sum()
        )
        consumo_diario = saidas_90 / 90 if saidas_90 > 0 else 0.0
        cobertura_dias = saldo / consumo_diario if consumo_diario > 0 else 0.0

        return {
            "saldo": saldo,
            "entradas": entradas,
            "saidas": saidas,
            "cobertura_dias": round(float(cobertura_dias), 1),
        }

    def get_estoque_dataframe(self):
        df = self._movimentos_dataframe()
        if df.empty:
            return pd.DataFrame(
                columns=[
                    "material_id",
                    "sku",
                    "descricao",
                    "categoria",
                    "unidade",
                    "estoque_minimo",
                    "lead_time_dias",
                    "saldo",
                ]
            )

        estoque = (
            df.groupby(
                [
                    "material_id",
                    "sku",
                    "descricao",
                    "categoria",
                    "unidade",
                    "estoque_minimo",
                    "lead_time_dias",
                ],
                as_index=False,
            )
            .agg(saldo=("qtd_ajustada", "sum"))
        )
        return estoque

    def get_top_produtos(self, top_n: int = 10):
        df = self._movimentos_dataframe()
        if df.empty:
            return pd.DataFrame(columns=["sku", "descricao", "quantidade", "valor"]) 

        saidas = df[df["tipo"] == "SAIDA"].copy()
        saidas["valor"] = saidas["quantidade"] * saidas["custo_unit"]

        top = (
            saidas.groupby(["sku", "descricao"], as_index=False)
            .agg(quantidade=("quantidade", "sum"), valor=("valor", "sum"))
            .sort_values("valor", ascending=False)
            .head(top_n)
        )
        return top
