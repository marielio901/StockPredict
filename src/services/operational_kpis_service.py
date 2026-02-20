import datetime as dt

import pandas as pd
from sqlalchemy.orm import Session

from src.models.schemas import Material, MovimentoEstoque


class OperationalKPIsService:
    DEFAULT_WINDOW_DAYS = 120

    def __init__(self, db: Session):
        self.db = db

    def _movimentos_dataframe(self) -> pd.DataFrame:
        query = (
            self.db.query(
                MovimentoEstoque.material_id,
                MovimentoEstoque.tipo,
                MovimentoEstoque.quantidade,
                MovimentoEstoque.data_mov,
                Material.sku,
                Material.descricao,
                Material.categoria,
            )
            .join(Material, Material.id == MovimentoEstoque.material_id)
            .statement
        )

        df = pd.read_sql(query, self.db.bind)
        if df.empty:
            return df

        df["data_mov"] = pd.to_datetime(df["data_mov"], errors="coerce")
        df = df.dropna(subset=["data_mov"]).copy()
        df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0.0)
        df["categoria"] = df["categoria"].fillna("Sem categoria")
        df["descricao"] = df["descricao"].fillna("")
        df["sku"] = df["sku"].fillna("")
        df["data"] = df["data_mov"].dt.floor("D")
        df["signed_qty"] = df.apply(
            lambda row: -row["quantidade"] if row["tipo"] == "SAIDA" else row["quantidade"],
            axis=1,
        )
        return df

    def get_filter_options(self):
        df = self._movimentos_dataframe()
        today = dt.date.today()
        if df.empty:
            return {
                "categorias": [],
                "date_min": today - dt.timedelta(days=30),
                "date_max": today,
            }

        return {
            "categorias": sorted([c for c in df["categoria"].dropna().unique().tolist() if str(c).strip()]),
            "date_min": df["data"].min().date(),
            "date_max": df["data"].max().date(),
        }

    def _resolve_date_range(self, df: pd.DataFrame, date_start=None, date_end=None):
        today = dt.date.today()
        if df.empty:
            end = date_end or today
            start = date_start or (end - dt.timedelta(days=self.DEFAULT_WINDOW_DAYS - 1))
            if start > end:
                start, end = end, start
            return start, end

        min_date = df["data"].min().date()
        max_date = df["data"].max().date()

        if date_end is None:
            date_end = max_date
        if date_start is None:
            date_start = max(min_date, date_end - dt.timedelta(days=self.DEFAULT_WINDOW_DAYS - 1))

        date_start = max(min_date, min(date_start, max_date))
        date_end = max(min_date, min(date_end, max_date))

        if date_start > date_end:
            date_start, date_end = date_end, date_start

        return date_start, date_end

    def _apply_category_filter(self, df: pd.DataFrame, categorias=None) -> pd.DataFrame:
        categorias = categorias or []
        if not categorias:
            return df.copy()
        return df[df["categoria"].isin(categorias)].copy()

    def _period_filter(self, df: pd.DataFrame, date_start: dt.date, date_end: dt.date) -> pd.DataFrame:
        if df.empty:
            return df.copy()
        mask = (df["data"].dt.date >= date_start) & (df["data"].dt.date <= date_end)
        return df[mask].copy()

    def _series_snapshot(self, series: pd.Series, higher_better: bool, window: int = 7):
        clean = pd.to_numeric(series, errors="coerce").dropna()
        if clean.empty:
            return {"current": 0.0, "delta": 0.0, "trend": "stable", "tone": "neutral"}

        window = max(1, min(window, len(clean)))
        current = float(clean.tail(window).mean())

        prev_slice = clean.iloc[-2 * window : -window]
        previous = float(prev_slice.mean()) if not prev_slice.empty else current
        delta = current - previous

        if abs(delta) < 1e-9:
            trend = "stable"
        elif delta > 0:
            trend = "up"
        else:
            trend = "down"

        tolerance = max(0.08, abs(previous) * 0.02)
        if abs(delta) <= tolerance:
            tone = "neutral"
        else:
            improved = delta >= 0 if higher_better else delta <= 0
            tone = "good" if improved else "danger"

        return {"current": current, "delta": delta, "trend": trend, "tone": tone}

    def _daily_qty(self, df: pd.DataFrame, tipo: str, idx: pd.DatetimeIndex, absolute: bool = False) -> pd.Series:
        if df.empty:
            return pd.Series(0.0, index=idx)

        part = df[df["tipo"] == tipo].copy()
        if part.empty:
            return pd.Series(0.0, index=idx)

        part["_qty"] = pd.to_numeric(part["quantidade"], errors="coerce").fillna(0.0)
        if absolute:
            part["_qty"] = part["_qty"].abs()

        return part.groupby("data")["_qty"].sum().reindex(idx, fill_value=0.0).astype(float)

    def _build_stock_series(
        self,
        df_full: pd.DataFrame,
        df_period: pd.DataFrame,
        date_start: dt.date,
        idx_period: pd.DatetimeIndex,
    ) -> pd.DataFrame:
        if df_period.empty and df_full.empty:
            return pd.DataFrame(index=idx_period)

        pre = df_full[df_full["data"].dt.date < date_start]
        pre_balance = pre.groupby("sku")["signed_qty"].sum() if not pre.empty else pd.Series(dtype=float)

        moves = (
            df_period.groupby(["data", "sku"], as_index=False)["signed_qty"]
            .sum()
            .pivot(index="data", columns="sku", values="signed_qty")
            if not df_period.empty
            else pd.DataFrame(index=idx_period)
        )

        all_skus = sorted(set(pre_balance.index.tolist()) | set(moves.columns.tolist()))
        if not all_skus:
            return pd.DataFrame(index=idx_period)

        moves = moves.reindex(index=idx_period, columns=all_skus, fill_value=0.0)
        opening = pd.Series(0.0, index=all_skus).add(pre_balance, fill_value=0.0)

        stock = moves.cumsum().add(opening, axis=1)
        return stock

    def get_performance_indicators(self, categorias=None, date_start=None, date_end=None, dead_days: int = 60):
        base_df = self._movimentos_dataframe()
        if base_df.empty:
            start, end = self._resolve_date_range(base_df, date_start, date_end)
            idx = pd.date_range(start, end, freq="D")
            empty_series = pd.Series(0.0, index=idx)
            return {
                "cards": {
                    "acuracidade": self._series_snapshot(empty_series, higher_better=True),
                    "divergencia": self._series_snapshot(empty_series, higher_better=False),
                    "giro": self._series_snapshot(empty_series, higher_better=True),
                    "cobertura": self._series_snapshot(empty_series, higher_better=True),
                    "ruptura": self._series_snapshot(empty_series, higher_better=False),
                    "dead_stock": self._series_snapshot(empty_series, higher_better=False),
                },
                "series": {
                    "acuracidade": pd.DataFrame({"data": idx, "valor": 0.0}),
                    "divergencia": pd.DataFrame({"data": idx, "valor": 0.0}),
                    "giro": pd.DataFrame({"data": idx, "valor": 0.0}),
                    "cobertura": pd.DataFrame({"data": idx, "valor": 0.0}),
                    "ruptura": pd.DataFrame({"data": idx, "valor": 0.0}),
                    "divergencia_categoria": pd.DataFrame(columns=["categoria", "valor"]),
                    "dead_stock_categoria": pd.DataFrame(columns=["categoria", "valor"]),
                    "dead_stock_faixa": pd.DataFrame(columns=["faixa", "valor"]),
                },
                "meta": {"date_start": start, "date_end": end, "dead_days": dead_days, "item_count": 0},
            }

        cat_df = self._apply_category_filter(base_df, categorias=categorias)
        date_start, date_end = self._resolve_date_range(cat_df if not cat_df.empty else base_df, date_start, date_end)
        period_df = self._period_filter(cat_df, date_start=date_start, date_end=date_end)

        idx = pd.date_range(date_start, date_end, freq="D")
        entradas = self._daily_qty(period_df, "ENTRADA", idx, absolute=True)
        saidas = self._daily_qty(period_df, "SAIDA", idx, absolute=True)
        ajustes_abs = self._daily_qty(period_df, "AJUSTE", idx, absolute=True)

        fluxo = entradas + saidas + ajustes_abs
        divergencia = (ajustes_abs / fluxo.replace(0, pd.NA) * 100.0).fillna(0.0).clip(lower=0.0, upper=100.0)
        acuracidade = (100.0 - divergencia).clip(lower=0.0, upper=100.0)

        stock_by_sku = self._build_stock_series(cat_df, period_df, date_start=date_start, idx_period=idx)
        sku_count = max(1, len(stock_by_sku.columns))
        if stock_by_sku.empty:
            stock_total = pd.Series(0.0, index=idx)
            ruptura = pd.Series(0.0, index=idx)
        else:
            stock_total = stock_by_sku.clip(lower=0.0).sum(axis=1)
            ruptura = ((stock_by_sku <= 0).sum(axis=1) / sku_count * 100.0).clip(lower=0.0, upper=100.0)

        consumo_medio = saidas.rolling(30, min_periods=1).mean().replace(0, pd.NA)
        cobertura = (
            stock_total / consumo_medio
        ).replace([pd.NA, pd.NaT, float("inf"), -float("inf")], 0).fillna(0.0).clip(
            lower=0.0, upper=365.0
        )

        estoque_medio = stock_total.rolling(30, min_periods=1).mean().replace(0, pd.NA)
        consumo_30 = saidas.rolling(30, min_periods=1).sum()
        giro = (
            consumo_30 / estoque_medio
        ).replace([pd.NA, pd.NaT, float("inf"), -float("inf")], 0).fillna(0.0).clip(lower=0.0)

        history_df = cat_df[cat_df["data"].dt.date <= date_end].copy()
        if history_df.empty:
            dead_stock_pct = 0.0
            dead_by_cat = pd.DataFrame(columns=["categoria", "valor"])
            dead_by_age = pd.DataFrame(columns=["faixa", "valor"])
            item_count = 0
        else:
            sku_last = (
                history_df.sort_values("data")
                .groupby("sku", as_index=False)
                .agg(last_mov=("data", "max"), categoria=("categoria", "last"))
            )
            sku_last["days_inactive"] = (pd.Timestamp(date_end) - sku_last["last_mov"]).dt.days
            sku_last["is_dead"] = sku_last["days_inactive"] >= int(dead_days)
            item_count = int(len(sku_last))
            dead_stock_pct = float((sku_last["is_dead"].sum() / max(1, item_count)) * 100.0)

            by_cat = (
                sku_last.groupby("categoria", as_index=False)
                .agg(total=("is_dead", "size"), dead=("is_dead", "sum"))
            )
            by_cat["valor"] = (by_cat["dead"] / by_cat["total"] * 100.0).fillna(0.0)
            dead_by_cat = by_cat[["categoria", "valor"]].sort_values("valor", ascending=False)

            bins = [0, 30, 60, 90, 180, 365, 10000]
            labels = ["0-30", "31-60", "61-90", "91-180", "181-365", ">365"]
            sku_last["faixa"] = pd.cut(
                sku_last["days_inactive"],
                bins=bins,
                labels=labels,
                include_lowest=True,
                right=True,
            )
            dead_by_age = (
                sku_last.groupby("faixa", as_index=False, observed=False)
                .size()
                .rename(columns={"size": "valor"})
            )

        dead_series = pd.Series(dead_stock_pct, index=idx)

        if period_df.empty:
            div_by_cat = pd.DataFrame(columns=["categoria", "valor"])
        else:
            aux = period_df.copy()
            aux["abs_qty"] = aux["quantidade"].abs()
            flow_cat = aux.groupby("categoria")["abs_qty"].sum()
            adj_cat = aux[aux["tipo"] == "AJUSTE"].groupby("categoria")["abs_qty"].sum()
            div_cat = ((adj_cat / flow_cat.replace(0, pd.NA)) * 100.0).fillna(0.0)
            div_by_cat = div_cat.rename("valor").reset_index()
            div_by_cat = div_by_cat.sort_values("valor", ascending=False)

        cards = {
            "acuracidade": self._series_snapshot(acuracidade, higher_better=True),
            "divergencia": self._series_snapshot(divergencia, higher_better=False),
            "giro": self._series_snapshot(giro, higher_better=True),
            "cobertura": self._series_snapshot(cobertura, higher_better=True),
            "ruptura": self._series_snapshot(ruptura, higher_better=False),
            "dead_stock": self._series_snapshot(dead_series, higher_better=False),
        }

        return {
            "cards": cards,
            "series": {
                "acuracidade": pd.DataFrame({"data": idx, "valor": acuracidade.values}),
                "divergencia": pd.DataFrame({"data": idx, "valor": divergencia.values}),
                "giro": pd.DataFrame({"data": idx, "valor": giro.values}),
                "cobertura": pd.DataFrame({"data": idx, "valor": cobertura.values}),
                "ruptura": pd.DataFrame({"data": idx, "valor": ruptura.values}),
                "divergencia_categoria": div_by_cat,
                "dead_stock_categoria": dead_by_cat,
                "dead_stock_faixa": dead_by_age,
            },
            "meta": {
                "date_start": date_start,
                "date_end": date_end,
                "dead_days": int(dead_days),
                "item_count": int(item_count),
            },
        }

    def get_armazenagem_indicators(
        self,
        categorias=None,
        date_start=None,
        date_end=None,
        shift_hours: float = 8.0,
    ):
        base_df = self._movimentos_dataframe()
        if base_df.empty:
            start, end = self._resolve_date_range(base_df, date_start, date_end)
            idx = pd.date_range(start, end, freq="D")
            empty = pd.Series(0.0, index=idx)
            return {
                "cards": {
                    "picking_rate": self._series_snapshot(empty, higher_better=True),
                    "picking_error": self._series_snapshot(empty, higher_better=False),
                    "dock_to_stock": self._series_snapshot(empty, higher_better=False),
                    "putaway": self._series_snapshot(empty, higher_better=False),
                },
                "series": {
                    "picking_rate": pd.DataFrame({"data": idx, "valor": 0.0}),
                    "picking_error": pd.DataFrame({"data": idx, "valor": 0.0}),
                    "dock_to_stock": pd.DataFrame({"data": idx, "valor": 0.0}),
                    "putaway": pd.DataFrame({"data": idx, "valor": 0.0}),
                },
                "meta": {"date_start": start, "date_end": end, "shift_hours": float(shift_hours)},
            }

        cat_df = self._apply_category_filter(base_df, categorias=categorias)
        date_start, date_end = self._resolve_date_range(cat_df if not cat_df.empty else base_df, date_start, date_end)
        period_df = self._period_filter(cat_df, date_start=date_start, date_end=date_end)

        idx = pd.date_range(date_start, date_end, freq="D")
        shift_hours = max(1.0, float(shift_hours or 8.0))

        saidas = self._daily_qty(period_df, "SAIDA", idx, absolute=True)
        ajustes_abs = self._daily_qty(period_df, "AJUSTE", idx, absolute=True)

        picking_rate = (saidas / shift_hours).fillna(0.0).clip(lower=0.0)
        picking_error = (ajustes_abs / saidas.replace(0, pd.NA) * 100.0).fillna(0.0).clip(lower=0.0, upper=100.0)

        entradas = period_df[period_df["tipo"] == "ENTRADA"].copy()
        if entradas.empty:
            dock_hours = pd.Series(0.0, index=idx)
            putaway_hours = pd.Series(0.0, index=idx)
        else:
            rec_qty = entradas.groupby("data")["quantidade"].sum().reindex(idx, fill_value=0.0)
            rec_docs = entradas.groupby("data").size().reindex(idx, fill_value=0.0).astype(float)
            rec_skus = entradas.groupby("data")["sku"].nunique().reindex(idx, fill_value=0.0).astype(float)

            qty_ref = max(float(rec_qty.quantile(0.9)), 1.0)
            docs_ref = max(float(rec_docs.quantile(0.9)), 1.0)
            skus_ref = max(float(rec_skus.quantile(0.9)), 1.0)

            workload = (
                0.45 * (rec_qty / qty_ref).clip(lower=0.0, upper=2.5)
                + 0.35 * (rec_docs / docs_ref).clip(lower=0.0, upper=2.5)
                + 0.20 * (rec_skus / skus_ref).clip(lower=0.0, upper=2.5)
            )

            dock_hours = (1.8 + workload * 3.8).clip(lower=0.5, upper=24.0)
            putaway_hours = (0.65 + workload * 2.4).clip(lower=0.25, upper=12.0)

        cards = {
            "picking_rate": self._series_snapshot(picking_rate, higher_better=True),
            "picking_error": self._series_snapshot(picking_error, higher_better=False),
            "dock_to_stock": self._series_snapshot(dock_hours, higher_better=False),
            "putaway": self._series_snapshot(putaway_hours, higher_better=False),
        }

        return {
            "cards": cards,
            "series": {
                "picking_rate": pd.DataFrame({"data": idx, "valor": picking_rate.values}),
                "picking_error": pd.DataFrame({"data": idx, "valor": picking_error.values}),
                "dock_to_stock": pd.DataFrame({"data": idx, "valor": dock_hours.values}),
                "putaway": pd.DataFrame({"data": idx, "valor": putaway_hours.values}),
            },
            "meta": {
                "date_start": date_start,
                "date_end": date_end,
                "shift_hours": shift_hours,
            },
        }
