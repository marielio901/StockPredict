import datetime as dt

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.models.schemas import Lote, Material, MovimentoEstoque

try:
    from prophet import Prophet
except Exception:
    Prophet = None


class ForecastService:
    MODEL_PROPHET = "Prophet"
    MODEL_MEDIA_7 = "Media7"
    MODEL_MEDIA_14 = "Media14"
    MODEL_ULTIMO = "UltimoValor"

    def __init__(self, db: Session):
        self.db = db

    def available_models(self):
        models = [self.MODEL_MEDIA_7, self.MODEL_MEDIA_14, self.MODEL_ULTIMO]
        if Prophet is not None:
            return [self.MODEL_PROPHET] + models
        return models

    def default_model(self):
        return self.MODEL_PROPHET if Prophet is not None else self.MODEL_MEDIA_7

    @staticmethod
    def _clamp(value: float, low: float, high: float) -> float:
        return max(float(low), min(float(high), float(value)))

    def _prepare_daily_series(self, series_df: pd.DataFrame) -> pd.DataFrame:
        if series_df is None or series_df.empty:
            return pd.DataFrame(columns=["ds", "y"])

        ser = series_df.copy()
        if "ds" not in ser.columns or "y" not in ser.columns:
            return pd.DataFrame(columns=["ds", "y"])

        ser["ds"] = pd.to_datetime(ser["ds"], errors="coerce")
        ser["y"] = pd.to_numeric(ser["y"], errors="coerce").fillna(0.0).clip(lower=0.0)
        ser = ser.dropna(subset=["ds"]).groupby("ds", as_index=False)["y"].sum().sort_values("ds")
        if ser.empty:
            return pd.DataFrame(columns=["ds", "y"])

        dense_idx = pd.date_range(ser["ds"].min(), ser["ds"].max(), freq="D")
        dense = (
            ser.set_index("ds")
            .reindex(dense_idx, fill_value=0.0)
            .rename_axis("ds")
            .reset_index()
        )
        dense["y"] = dense["y"].clip(lower=0.0)
        return dense

    def _estimate_demand_profile(self, series_df: pd.DataFrame, periods: int) -> dict:
        periods = int(max(1, periods))
        dense = self._prepare_daily_series(series_df)
        if dense.empty:
            return {
                "base_daily": 0.0,
                "low_daily": 0.0,
                "high_daily": 0.0,
                "demand_base": 0.0,
                "demand_low": 0.0,
                "demand_high": 0.0,
                "daily_cap": 0.0,
                "intermittency": 0.0,
            }

        y = pd.to_numeric(dense["y"], errors="coerce").fillna(0.0).clip(lower=0.0)
        nz = y[y > 0]
        if nz.empty:
            return {
                "base_daily": 0.0,
                "low_daily": 0.0,
                "high_daily": 0.0,
                "demand_base": 0.0,
                "demand_low": 0.0,
                "demand_high": 0.0,
                "daily_cap": 0.0,
                "intermittency": 0.0,
            }

        q1 = float(nz.quantile(0.25))
        q3 = float(nz.quantile(0.75))
        q90 = float(nz.quantile(0.90))
        q95 = float(nz.quantile(0.95))
        iqr = max(q3 - q1, 0.0)
        daily_cap = max(q95 * 1.35, q3 + (1.5 * iqr), q90, 0.0)
        y_cap = y.clip(upper=daily_cap if daily_cap > 0 else None)

        tail14 = y_cap.tail(min(14, len(y_cap)))
        tail30 = y_cap.tail(min(30, len(y_cap)))
        tail60 = y_cap.tail(min(60, len(y_cap)))

        m14 = float(tail14.mean()) if len(tail14) else 0.0
        m30 = float(tail30.mean()) if len(tail30) else m14
        m60 = float(tail60.mean()) if len(tail60) else m30
        base_daily = (0.45 * m14) + (0.35 * m30) + (0.20 * m60)

        if len(y_cap) >= 60:
            recent = float(y_cap.tail(30).mean())
            prev = float(y_cap.tail(60).head(30).mean())
        elif len(y_cap) >= 30:
            recent = float(y_cap.tail(15).mean())
            prev = float(y_cap.tail(30).head(15).mean())
        else:
            recent = float(y_cap.mean())
            prev = recent
        if prev > 0:
            trend_factor = self._clamp(recent / prev, 0.80, 1.25)
            base_daily *= trend_factor

        window = y_cap.tail(min(90, len(y_cap)))
        non_zero_days = int((window > 0).sum())
        intermittency = float(non_zero_days / max(len(window), 1))
        sparse_factor = self._clamp(intermittency * 1.15, 0.45, 1.0)
        base_daily *= sparse_factor

        mean_w = float(window.mean()) if len(window) else 0.0
        std_w = float(window.std(ddof=0)) if len(window) else 0.0
        cv = (std_w / mean_w) if mean_w > 0 else 0.0
        band = self._clamp(0.18 + (0.25 * cv), 0.15, 0.50)

        low_daily = max(base_daily * (1.0 - band), 0.0)
        high_daily = max(base_daily * (1.0 + band), low_daily)

        return {
            "base_daily": float(base_daily),
            "low_daily": float(low_daily),
            "high_daily": float(high_daily),
            "demand_base": float(base_daily * periods),
            "demand_low": float(low_daily * periods),
            "demand_high": float(high_daily * periods),
            "daily_cap": float(daily_cap),
            "intermittency": float(intermittency),
        }

    def _guardrailed_forecast_totals(
        self,
        history_series: pd.DataFrame,
        future_forecast: pd.DataFrame,
        periods: int,
    ) -> dict:
        periods = int(max(1, periods))
        profile = self._estimate_demand_profile(history_series, periods=periods)
        if future_forecast is None or future_forecast.empty:
            return profile

        fut = future_forecast.copy()
        fut["ds"] = pd.to_datetime(fut["ds"], errors="coerce")
        fut = fut.dropna(subset=["ds"]).sort_values("ds")
        fut = fut.tail(periods)
        if fut.empty:
            return profile

        cap = max(
            float(profile["high_daily"] * 2.0),
            float(profile["base_daily"] * 2.8),
            float(profile["daily_cap"]),
        )

        yhat = pd.to_numeric(fut.get("yhat", 0.0), errors="coerce").fillna(0.0).clip(lower=0.0)
        ylow = pd.to_numeric(fut.get("yhat_lower", yhat), errors="coerce").fillna(0.0).clip(lower=0.0)
        yhigh = pd.to_numeric(fut.get("yhat_upper", yhat), errors="coerce").fillna(0.0).clip(lower=0.0)

        if cap > 0:
            yhat = yhat.clip(upper=cap)
            ylow = ylow.clip(upper=cap)
            yhigh = yhigh.clip(upper=cap)

        base_ref = float(profile["base_daily"])
        low_ref = float(profile["low_daily"])
        high_ref = float(profile["high_daily"])

        yhat = (yhat * 0.70) + (base_ref * 0.30)
        ylow = (ylow * 0.70) + (low_ref * 0.30)
        yhigh = (yhigh * 0.70) + (high_ref * 0.30)
        ylow = ylow.clip(lower=0.0)
        yhigh = yhigh.clip(lower=ylow)

        demand_base_model = float(yhat.sum())
        demand_low_model = float(ylow.sum())
        demand_high_model = float(yhigh.sum())

        demand_base = (demand_base_model * 0.75) + (float(profile["demand_base"]) * 0.25)
        demand_low = (demand_low_model * 0.75) + (float(profile["demand_low"]) * 0.25)
        demand_high = (demand_high_model * 0.75) + (float(profile["demand_high"]) * 0.25)

        return {
            "base_daily": float(demand_base / periods),
            "low_daily": float(demand_low / periods),
            "high_daily": float(demand_high / periods),
            "demand_base": float(max(demand_base, 0.0)),
            "demand_low": float(max(demand_low, 0.0)),
            "demand_high": float(max(demand_high, 0.0)),
            "daily_cap": float(profile["daily_cap"]),
            "intermittency": float(profile["intermittency"]),
        }

    def get_historical_series(self, material_id: int) -> pd.DataFrame:
        df = self._load_daily_saida(material_id)
        if df.empty:
            return pd.DataFrame(columns=["ds", "y"])
        return df.copy()

    def _load_daily_saida(self, material_id: int) -> pd.DataFrame:
        query = (
            self.db.query(MovimentoEstoque.data_mov, MovimentoEstoque.quantidade)
            .filter(MovimentoEstoque.material_id == material_id)
            .filter(MovimentoEstoque.tipo == "SAIDA")
            .statement
        )

        df = pd.read_sql(query, self.db.bind)
        if df.empty:
            return pd.DataFrame(columns=["ds", "y"])

        df.columns = ["ds", "y"]
        df["ds"] = pd.to_datetime(df["ds"], errors="coerce")
        df["y"] = pd.to_numeric(df["y"], errors="coerce").fillna(0.0)
        df = df.dropna(subset=["ds"]).groupby("ds", as_index=False)["y"].sum()
        df = df.sort_values("ds")
        if df.empty:
            return pd.DataFrame(columns=["ds", "y"])

        dense_idx = pd.date_range(df["ds"].min(), df["ds"].max(), freq="D")
        dense = (
            df.set_index("ds")
            .reindex(dense_idx, fill_value=0.0)
            .rename_axis("ds")
            .reset_index()
        )
        dense["y"] = dense["y"].clip(lower=0.0)
        return dense

    def _forecast_constant(self, train_df: pd.DataFrame, periods: int, base_value: float):
        hist = train_df[["ds", "y"]].copy()
        hist.columns = ["ds", "yhat"]
        hist["yhat"] = hist["yhat"].clip(lower=0.0)
        hist["yhat_lower"] = hist["yhat"]
        hist["yhat_upper"] = hist["yhat"]

        base = max(float(base_value or 0.0), 0.0)
        last_date = train_df["ds"].max()
        future_dates = pd.date_range(
            start=last_date + pd.Timedelta(days=1), periods=periods, freq="D"
        )
        future = pd.DataFrame({"ds": future_dates})
        future["yhat"] = base
        future["yhat_lower"] = max(0.0, base * 0.8)
        future["yhat_upper"] = max(0.0, base * 1.2)

        return pd.concat(
            [hist[["ds", "yhat", "yhat_lower", "yhat_upper"]], future],
            ignore_index=True,
        )

    def _forecast_prophet(self, train_df: pd.DataFrame, periods: int):
        if Prophet is None or train_df.shape[0] < 2:
            base = float(train_df["y"].tail(min(7, len(train_df))).mean()) if len(train_df) else 0.0
            return self._forecast_constant(train_df, periods, base)

        try:
            model = Prophet(
                daily_seasonality=True,
                weekly_seasonality=True,
                yearly_seasonality=False,
            )
            model.fit(train_df)
            future = model.make_future_dataframe(periods=periods)
            forecast = model.predict(future)
            forecast = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
            for col in ["yhat", "yhat_lower", "yhat_upper"]:
                forecast[col] = pd.to_numeric(forecast[col], errors="coerce").fillna(0.0).clip(lower=0.0)
            return forecast
        except Exception:
            base = float(train_df["y"].tail(min(7, len(train_df))).mean()) if len(train_df) else 0.0
            return self._forecast_constant(train_df, periods, base)

    def _forecast_model(self, train_df: pd.DataFrame, periods: int, model_name: str):
        model_key = str(model_name or "").strip().lower()

        if model_key == self.MODEL_PROPHET.lower():
            return self._forecast_prophet(train_df, periods)
        if model_key == self.MODEL_MEDIA_14.lower():
            base = float(train_df["y"].tail(min(14, len(train_df))).mean()) if len(train_df) else 0.0
            return self._forecast_constant(train_df, periods, base)
        if model_key == self.MODEL_ULTIMO.lower():
            base = float(train_df["y"].iloc[-1]) if len(train_df) else 0.0
            return self._forecast_constant(train_df, periods, base)

        base = float(train_df["y"].tail(min(7, len(train_df))).mean()) if len(train_df) else 0.0
        return self._forecast_constant(train_df, periods, base)

    def train_forecast(self, material_id: int, periods=30, model_name: str | None = None):
        periods = int(max(1, periods))
        series = self._load_daily_saida(material_id)
        if series.empty:
            return None, "Sem dados de saida para previsao."

        model_name = model_name or self.default_model()
        forecast = self._forecast_model(series, periods, model_name)
        if forecast is not None and not forecast.empty:
            forecast = forecast.copy()
            forecast["ds"] = pd.to_datetime(forecast["ds"], errors="coerce")
            forecast = forecast.dropna(subset=["ds"]).sort_values("ds")
            last_hist = pd.to_datetime(series["ds"], errors="coerce").max()
            future_mask = forecast["ds"] > last_hist
            if future_mask.any():
                profile = self._estimate_demand_profile(series, periods=periods)
                cap = max(
                    float(profile["high_daily"] * 2.0),
                    float(profile["base_daily"] * 2.8),
                    float(profile["daily_cap"]),
                )
                for col in ["yhat", "yhat_lower", "yhat_upper"]:
                    if col not in forecast.columns:
                        forecast[col] = forecast["yhat"] if "yhat" in forecast.columns else 0.0
                    forecast[col] = pd.to_numeric(forecast[col], errors="coerce").fillna(0.0).clip(lower=0.0)
                    if cap > 0:
                        forecast[col] = forecast[col].clip(upper=cap)

                base_ref = float(profile["base_daily"])
                low_ref = float(profile["low_daily"])
                high_ref = float(profile["high_daily"])
                forecast.loc[future_mask, "yhat"] = (
                    forecast.loc[future_mask, "yhat"] * 0.70
                ) + (base_ref * 0.30)
                forecast.loc[future_mask, "yhat_lower"] = (
                    forecast.loc[future_mask, "yhat_lower"] * 0.70
                ) + (low_ref * 0.30)
                forecast.loc[future_mask, "yhat_upper"] = (
                    forecast.loc[future_mask, "yhat_upper"] * 0.70
                ) + (high_ref * 0.30)
                forecast["yhat_lower"] = forecast["yhat_lower"].clip(lower=0.0)
                forecast["yhat_upper"] = forecast["yhat_upper"].clip(lower=forecast["yhat_lower"])
        return forecast, None

    @staticmethod
    def _calc_metrics(actual, predicted):
        actual_s = pd.Series(actual, dtype=float).fillna(0.0)
        pred_s = pd.Series(predicted, dtype=float).fillna(0.0)
        if len(actual_s) == 0:
            return {
                "MAE": 0.0,
                "RMSE": 0.0,
                "MAPE (%)": None,
                "WMAPE (%)": None,
                "Total Real": 0.0,
                "Total Previsto": 0.0,
                "Erro Total (%)": None,
            }

        err = pred_s - actual_s
        mae = float(err.abs().mean())
        rmse = float((err.pow(2).mean()) ** 0.5)

        mask = actual_s > 0
        if mask.any():
            mape = float(((err.abs()[mask] / actual_s[mask]).mean()) * 100.0)
        else:
            mape = None

        total_real = float(actual_s.sum())
        total_prev = float(pred_s.sum())
        if total_real > 0:
            erro_total_pct = float(((total_prev - total_real) / total_real) * 100.0)
            wmape = float((err.abs().sum() / total_real) * 100.0)
        else:
            erro_total_pct = None
            wmape = None

        return {
            "MAE": mae,
            "RMSE": rmse,
            "MAPE (%)": mape,
            "WMAPE (%)": wmape,
            "Total Real": total_real,
            "Total Previsto": total_prev,
            "Erro Total (%)": erro_total_pct,
        }

    def benchmark_models(self, material_id: int, horizon=30):
        series = self._load_daily_saida(material_id)
        if series.empty:
            return pd.DataFrame(), pd.DataFrame(), "Sem dados para benchmark."

        horizon = int(max(7, horizon))
        if len(series) < 28:
            return pd.DataFrame(), pd.DataFrame(), "Historico insuficiente para benchmark."

        if len(series) - horizon < 21:
            horizon = max(7, len(series) - 21)
        if horizon < 7:
            return pd.DataFrame(), pd.DataFrame(), "Historico insuficiente para benchmark."

        train = series.iloc[:-horizon].copy()
        test = series.iloc[-horizon:].copy()
        models = self.available_models()

        benchmark_rows = []
        curves = pd.DataFrame({"ds": test["ds"], "Real": test["y"].values})

        for model_name in models:
            forecast = self._forecast_model(train, horizon, model_name)
            pred = pd.to_numeric(forecast.tail(horizon)["yhat"], errors="coerce").fillna(0.0).values
            metrics = self._calc_metrics(test["y"].values, pred)
            benchmark_rows.append(
                {
                    "Modelo": model_name,
                    "MAE": metrics["MAE"],
                    "RMSE": metrics["RMSE"],
                    "MAPE (%)": metrics["MAPE (%)"],
                    "WMAPE (%)": metrics["WMAPE (%)"],
                    "Total Real": metrics["Total Real"],
                    "Total Previsto": metrics["Total Previsto"],
                    "Erro Total (%)": metrics["Erro Total (%)"],
                }
            )
            curves[model_name] = pred

        bench_df = pd.DataFrame(benchmark_rows).sort_values("MAE", ascending=True)
        return bench_df, curves, None

    def backtest_windows(self, material_id: int, model_name: str, horizon=14, windows=4):
        series = self._load_daily_saida(material_id)
        if series.empty:
            return pd.DataFrame(), "Sem dados para backtest."

        horizon = int(max(7, horizon))
        windows = int(max(1, windows))

        if len(series) < horizon + 21:
            return pd.DataFrame(), "Historico insuficiente para backtest."

        max_windows = max(1, (len(series) - 21) // horizon)
        windows = min(windows, max_windows)

        rows = []
        for idx in range(windows):
            test_end = len(series) - (idx * horizon)
            test_start = test_end - horizon
            train_end = test_start
            if train_end < 21:
                continue

            train = series.iloc[:train_end].copy()
            test = series.iloc[test_start:test_end].copy()
            forecast = self._forecast_model(train, len(test), model_name)
            pred = pd.to_numeric(forecast.tail(len(test))["yhat"], errors="coerce").fillna(0.0).values
            metrics = self._calc_metrics(test["y"].values, pred)

            rows.append(
                {
                    "Janela": windows - idx,
                    "Treino ate": train["ds"].max().date(),
                    "Teste inicio": test["ds"].min().date(),
                    "Teste fim": test["ds"].max().date(),
                    "Total Real": metrics["Total Real"],
                    "Total Previsto": metrics["Total Previsto"],
                    "Erro Total (%)": metrics["Erro Total (%)"],
                    "WMAPE (%)": metrics["WMAPE (%)"],
                    "MAE": metrics["MAE"],
                    "RMSE": metrics["RMSE"],
                }
            )

        if not rows:
            return pd.DataFrame(), "Nao foi possivel montar janelas de backtest."

        bt_df = pd.DataFrame(rows).sort_values("Janela")
        return bt_df, None

    def get_material_snapshot(self, material_id: int, periods: int, future_forecast: pd.DataFrame | None = None):
        material = self.db.query(Material).filter(Material.id == material_id).first()
        if not material:
            return None

        moves = (
            self.db.query(
                MovimentoEstoque.tipo,
                func.sum(MovimentoEstoque.quantidade).label("total"),
            )
            .filter(MovimentoEstoque.material_id == material_id)
            .group_by(MovimentoEstoque.tipo)
            .all()
        )
        totals = {r.tipo: float(r.total or 0.0) for r in moves}
        entradas = totals.get("ENTRADA", 0.0)
        saidas = totals.get("SAIDA", 0.0)
        ajustes = totals.get("AJUSTE", 0.0)
        estoque_atual = max(entradas - saidas + ajustes, 0.0)

        estoque_minimo = max(float(material.estoque_minimo or 0.0), 10.0)
        periods = int(max(1, periods))
        lead_time_dias = int(max(material.lead_time_dias or 7, 1))
        hist_series = self._load_daily_saida(material_id)
        if future_forecast is not None and not future_forecast.empty:
            profile = self._guardrailed_forecast_totals(
                history_series=hist_series,
                future_forecast=future_forecast,
                periods=periods,
            )
        else:
            profile = self._estimate_demand_profile(hist_series, periods=periods)

        demand_base = float(profile["demand_base"])
        demand_low = float(profile["demand_low"])
        demand_high = float(profile["demand_high"])

        horizon_buffer_days = min(lead_time_dias, periods)
        safety_buffer_base = float(profile["base_daily"] * horizon_buffer_days * 0.50)
        safety_buffer_low = float(profile["low_daily"] * horizon_buffer_days * 0.35)
        safety_buffer_high = float(profile["high_daily"] * horizon_buffer_days * 0.65)

        proj_best = estoque_atual - demand_low - safety_buffer_low
        proj_base = estoque_atual - demand_base - safety_buffer_base
        proj_worst = estoque_atual - demand_high - safety_buffer_high

        today = dt.date.today()
        horizon_date = today + dt.timedelta(days=periods)
        qtd_a_vencer = float(
            self.db.query(func.sum(Lote.quantidade))
            .filter(Lote.material_id == material_id)
            .filter(Lote.validade.isnot(None))
            .filter(Lote.validade <= horizon_date)
            .scalar()
            or 0.0
        )

        risco_ruptura = proj_worst <= 0
        risco_perda = (proj_best > (estoque_minimo * 2.2)) or (qtd_a_vencer > 0 and proj_base > 0)
        suficiente = proj_base >= estoque_minimo and not risco_perda

        return {
            "material_id": material.id,
            "sku": material.sku,
            "descricao": material.descricao,
            "familia": material.categoria,
            "lead_time_dias": lead_time_dias,
            "estoque_atual": float(estoque_atual),
            "estoque_minimo": float(estoque_minimo),
            "demanda_diaria_base": float(profile["base_daily"]),
            "demanda_diaria_otimista": float(profile["low_daily"]),
            "demanda_diaria_pessimista": float(profile["high_daily"]),
            "intermitencia": float(profile["intermittency"]),
            "buffer_seguranca": float(safety_buffer_base),
            "demanda_prevista": float(demand_base),
            "demanda_otimista": float(demand_low),
            "demanda_pessimista": float(demand_high),
            "estoque_proj_otimista": float(proj_best),
            "estoque_proj_base": float(proj_base),
            "estoque_proj_pessimista": float(proj_worst),
            "qtd_a_vencer": float(qtd_a_vencer),
            "cenario_ruptura": bool(risco_ruptura),
            "cenario_perda": bool(risco_perda),
            "cenario_suficiente": bool(suficiente),
        }

    def get_portfolio_scenarios(self, periods=30):
        periods = int(max(1, periods))
        materials = self.db.query(Material).all()
        if not materials:
            empty = pd.DataFrame()
            return {
                "ruptura": empty,
                "vencer": empty,
                "travados": empty,
                "perdas": empty,
                "suficientes": empty,
                "resumo": {"ruptura": 0, "vencer": 0, "travados": 0, "perdas": 0, "suficientes": 0},
            }

        move_query = (
            self.db.query(
                MovimentoEstoque.material_id,
                MovimentoEstoque.tipo,
                MovimentoEstoque.quantidade,
                MovimentoEstoque.data_mov,
            )
            .statement
        )
        df = pd.read_sql(move_query, self.db.bind)
        if df.empty:
            empty = pd.DataFrame()
            return {
                "ruptura": empty,
                "vencer": empty,
                "travados": empty,
                "perdas": empty,
                "suficientes": empty,
                "resumo": {"ruptura": 0, "vencer": 0, "travados": 0, "perdas": 0, "suficientes": 0},
            }

        df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce").fillna(0.0)
        df["data_mov"] = pd.to_datetime(df["data_mov"], errors="coerce")

        def signed(row):
            if row["tipo"] == "SAIDA":
                return -row["quantidade"]
            return row["quantidade"]

        df["qtd_signed"] = df.apply(signed, axis=1)
        estoque_atual = df.groupby("material_id", as_index=False)["qtd_signed"].sum()
        estoque_atual.columns = ["material_id", "estoque_atual"]

        saida_daily = (
            df[df["tipo"] == "SAIDA"][["material_id", "data_mov", "quantidade"]]
            .dropna(subset=["data_mov"])
            .groupby(["material_id", "data_mov"], as_index=False)["quantidade"]
            .sum()
        )
        profile_map: dict[int, dict] = {}
        if not saida_daily.empty:
            for mid, grp in saida_daily.groupby("material_id"):
                ser = grp.rename(columns={"data_mov": "ds", "quantidade": "y"})[["ds", "y"]]
                profile_map[int(mid)] = self._estimate_demand_profile(ser, periods=periods)

        zero_profile = {
            "base_daily": 0.0,
            "low_daily": 0.0,
            "high_daily": 0.0,
            "demand_base": 0.0,
            "demand_low": 0.0,
            "demand_high": 0.0,
            "daily_cap": 0.0,
            "intermittency": 0.0,
        }

        base = pd.DataFrame(
            [
                {
                    "material_id": m.id,
                    "SKU": m.sku,
                    "Descricao": m.descricao,
                    "Familia": m.categoria,
                    "Estoque Minimo": max(float(m.estoque_minimo or 0.0), 10.0),
                    "Lead Time": int(max(m.lead_time_dias or 7, 1)),
                }
                for m in materials
            ]
        )
        base = base.merge(estoque_atual, on="material_id", how="left")
        base["estoque_atual"] = (
            pd.to_numeric(base["estoque_atual"], errors="coerce")
            .fillna(0.0)
            .clip(lower=0.0)
        )
        base["profile"] = base["material_id"].map(lambda mid: profile_map.get(int(mid), zero_profile))
        base["demanda_prevista"] = base["profile"].map(lambda p: float(p["demand_base"]))
        base["demanda_otimista"] = base["profile"].map(lambda p: float(p["demand_low"]))
        base["demanda_pessimista"] = base["profile"].map(lambda p: float(p["demand_high"]))
        base["intermitencia"] = base["profile"].map(lambda p: float(p["intermittency"]))
        base["buffer_seguranca"] = base.apply(
            lambda r: float(r["profile"]["base_daily"]) * min(int(r["Lead Time"]), periods) * 0.50,
            axis=1,
        )
        base["estoque_projetado"] = base["estoque_atual"] - base["demanda_prevista"] - base["buffer_seguranca"]
        base["saldo_proj_pct"] = (
            (base["estoque_projetado"] / base["Estoque Minimo"]) * 100.0
        ).clip(lower=0.0, upper=100.0)

        today = dt.date.today()
        horizon_date = today + dt.timedelta(days=periods)
        exp_rows = (
            self.db.query(Lote.material_id, func.sum(Lote.quantidade))
            .filter(Lote.validade.isnot(None))
            .filter(Lote.validade <= horizon_date)
            .group_by(Lote.material_id)
            .all()
        )
        exp_map = {int(mid): float(qtd or 0.0) for mid, qtd in exp_rows}
        base["Qtd a Vencer"] = base["material_id"].map(exp_map).fillna(0.0)

        ruptura_df = base[base["estoque_projetado"] <= 0].copy()
        vencer_df = base[base["Qtd a Vencer"] > 0].copy()
        travados_df = base[
            (base["estoque_projetado"] > (base["Estoque Minimo"] * 2.2))
            & (base["demanda_prevista"] <= (base["Estoque Minimo"] * 0.45))
        ].copy()
        perdas_df = base[
            (base["estoque_projetado"] > (base["Estoque Minimo"] * 2.2))
            | ((base["Qtd a Vencer"] > 0) & (base["estoque_projetado"] > (base["Estoque Minimo"] * 0.8)))
        ].copy()
        suficientes_df = base[
            (base["estoque_projetado"] >= base["Estoque Minimo"])
            & (base["estoque_projetado"] <= (base["Estoque Minimo"] * 2.2))
            & (base["Qtd a Vencer"] <= 0)
        ].copy()

        display_cols = [
            "SKU",
            "Descricao",
            "Familia",
            "estoque_atual",
            "demanda_prevista",
            "estoque_projetado",
            "Estoque Minimo",
            "saldo_proj_pct",
            "Qtd a Vencer",
        ]
        rename_cols = {
            "estoque_atual": "Estoque Atual",
            "demanda_prevista": "Demanda Prevista",
            "estoque_projetado": "Estoque Projetado",
            "saldo_proj_pct": "Saldo Projetado (%)",
        }

        ruptura_df = ruptura_df[display_cols].rename(columns=rename_cols).sort_values(
            "Estoque Projetado", ascending=True
        )
        perdas_df = perdas_df[display_cols].rename(columns=rename_cols).sort_values(
            ["Qtd a Vencer", "Estoque Projetado"], ascending=[False, False]
        )
        vencer_df = vencer_df[display_cols].rename(columns=rename_cols).sort_values(
            ["Qtd a Vencer", "Saldo Projetado (%)"], ascending=[False, True]
        )
        travados_df = travados_df[display_cols].rename(columns=rename_cols).sort_values(
            "Estoque Projetado", ascending=False
        )
        suficientes_df = suficientes_df[display_cols].rename(columns=rename_cols).sort_values(
            "Saldo Projetado (%)", ascending=False
        )

        return {
            "ruptura": ruptura_df,
            "vencer": vencer_df,
            "travados": travados_df,
            "perdas": perdas_df,
            "suficientes": suficientes_df,
            "resumo": {
                "ruptura": int(len(ruptura_df)),
                "vencer": int(len(vencer_df)),
                "travados": int(len(travados_df)),
                "perdas": int(len(perdas_df)),
                "suficientes": int(len(suficientes_df)),
            },
        }

    def get_predictive_horizon_dashboard(self):
        horizons = [
            ("Proximo Mes", 30),
            ("Proximo Semestre", 180),
            ("Proximo Ano", 365),
        ]
        rows = []
        details = {}
        for label, days in horizons:
            scenarios = self.get_portfolio_scenarios(periods=days)
            resumo = scenarios.get("resumo", {})
            rows.append(
                {
                    "Horizonte": label,
                    "Dias": days,
                    "Ruptura": int(resumo.get("ruptura", 0)),
                    "Vencer": int(resumo.get("vencer", 0)),
                    "Travados": int(resumo.get("travados", 0)),
                    "Suficientes": int(resumo.get("suficientes", 0)),
                }
            )
            details[days] = scenarios

        return pd.DataFrame(rows), details

    def get_global_trends(self, history_days=180, forecast_days=30):
        history_days = int(max(30, history_days))
        forecast_days = int(max(7, forecast_days))

        query = (
            self.db.query(
                MovimentoEstoque.data_mov,
                MovimentoEstoque.tipo,
                func.sum(MovimentoEstoque.quantidade).label("qtd"),
            )
            .group_by(MovimentoEstoque.data_mov, MovimentoEstoque.tipo)
            .statement
        )
        df = pd.read_sql(query, self.db.bind)
        if df.empty:
            return pd.DataFrame(columns=["ds", "Entradas", "Saidas", "Entradas Prev", "Saidas Prev"])

        df["data_mov"] = pd.to_datetime(df["data_mov"], errors="coerce")
        df["qtd"] = pd.to_numeric(df["qtd"], errors="coerce").fillna(0.0)
        df = df.dropna(subset=["data_mov"])
        if df.empty:
            return pd.DataFrame(columns=["ds", "Entradas", "Saidas", "Entradas Prev", "Saidas Prev"])

        pivot = (
            df.pivot_table(
                index="data_mov",
                columns="tipo",
                values="qtd",
                aggfunc="sum",
                fill_value=0.0,
            )
            .sort_index()
        )

        for col in ["ENTRADA", "SAIDA", "AJUSTE"]:
            if col not in pivot.columns:
                pivot[col] = 0.0

        end_date = pivot.index.max()
        start_date = end_date - pd.Timedelta(days=history_days - 1)
        hist_idx = pd.date_range(start_date, end_date, freq="D")
        hist = pivot.reindex(hist_idx, fill_value=0.0)

        hist_out = pd.DataFrame(
            {
                "ds": hist.index,
                "Entradas": hist["ENTRADA"] + hist["AJUSTE"].clip(lower=0.0),
                "Saidas": hist["SAIDA"],
                "Entradas Prev": 0.0,
                "Saidas Prev": 0.0,
            }
        )

        def _daily_projection(series: pd.Series, horizon: int):
            vals = pd.to_numeric(series, errors="coerce").fillna(0.0).clip(lower=0.0)
            if vals.empty:
                return [0.0] * horizon

            nz = vals[vals > 0]
            if not nz.empty:
                q3 = float(nz.quantile(0.75))
                q95 = float(nz.quantile(0.95))
                cap = max(q95 * 1.25, q3 * 1.8)
                vals = vals.clip(upper=cap)

            m14 = float(vals.tail(min(14, len(vals))).mean())
            m30 = float(vals.tail(min(30, len(vals))).mean())
            base = (0.55 * m14) + (0.45 * m30)

            if len(vals) >= 28:
                recent = float(vals.tail(14).mean())
                prev = float(vals.tail(28).head(14).mean())
                drift = (recent - prev) / 14.0
            else:
                drift = 0.0
            drift_cap = max(base * 0.05, 0.0)
            drift = self._clamp(drift, -drift_cap, drift_cap)

            out = []
            for day in range(1, horizon + 1):
                out.append(max(base + (drift * day), 0.0))
            return out

        ent_prev = _daily_projection(hist_out["Entradas"], forecast_days)
        sai_prev = _daily_projection(hist_out["Saidas"], forecast_days)
        future_idx = pd.date_range(end_date + pd.Timedelta(days=1), periods=forecast_days, freq="D")
        fut = pd.DataFrame(
            {
                "ds": future_idx,
                "Entradas": 0.0,
                "Saidas": 0.0,
                "Entradas Prev": ent_prev,
                "Saidas Prev": sai_prev,
            }
        )

        return pd.concat([hist_out, fut], ignore_index=True)
