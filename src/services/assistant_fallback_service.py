import datetime as dt
import unicodedata
from typing import Any


class AssistantFallbackService:
    MONEY_HINTS = {
        "dinheiro",
        "valor",
        "financeiro",
        "reais",
        "saldo em dinheiro",
        "saldo financeiro",
        "valor estoque",
    }
    MOVEMENT_HINTS = {
        "movimentacao",
        "movimentacoes",
        "entrada",
        "entradas",
        "saida",
        "saidas",
        "ajuste",
        "consumo",
    }
    PREDICTIVE_HINTS = {
        "previsao",
        "preditiva",
        "predicao",
        "futuro",
        "horizonte",
        "cenario",
        "forecast",
        "demanda futura",
    }
    ITEM_HINTS = {
        "item",
        "itens",
        "sku",
        "cod",
        "codigo",
        "descricao",
        "saldo",
    }
    ALERT_HINTS = {
        "alerta",
        "alertas",
        "ruptura",
        "critic",
        "prioridade",
        "venc",
    }
    RISK_HINTS = {
        "ruptura",
        "zerar",
        "zero",
        "perto de zerar",
        "abaixo do minimo",
        "baixo estoque",
        "falta",
        "sem estoque",
    }
    LAST_HINTS = {
        "ultima",
        "ultimo",
        "mais recente",
        "recente",
        "latest",
    }

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
        if isinstance(value, dt.datetime):
            return value.strftime("%d/%m/%Y")
        if isinstance(value, dt.date):
            return value.strftime("%d/%m/%Y")
        return str(value)

    def _has_any(self, question_norm: str, words: set[str]) -> bool:
        return any(word in question_norm for word in words)

    def _answer_money(self, ctx: dict) -> str:
        finance = ctx.get("finance", {})
        inventory = ctx.get("inventory", {})

        lines = ["Saldo financeiro consolidado:"]
        lines.append(
            f"- Valor total em estoque: **{self._fmt_money(finance.get('saldo_total_valor', 0.0))}**"
        )
        lines.append(
            f"- Valor acumulado de entradas: **{self._fmt_money(finance.get('val_entrada_total', 0.0))}**"
        )
        lines.append(
            f"- Valor acumulado de saidas: **{self._fmt_money(finance.get('val_saida_total', 0.0))}**"
        )

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

        return "\n".join(lines)

    def _answer_movements(self, ctx: dict) -> str:
        movements = ctx.get("movements", {})
        lines = ["Movimentacoes do estoque:"]

        for key in ["d7", "d30", "d90"]:
            block = movements.get(key, {})
            lines.append(
                "- Ultimos {days} dias: entradas {ent} | saidas {sai} | ajustes {aju} | saldo liquido {liq}".format(
                    days=int(block.get("days", 0)),
                    ent=self._fmt_num(block.get("entradas_qtd", 0.0), dec=2),
                    sai=self._fmt_num(block.get("saidas_qtd", 0.0), dec=2),
                    aju=self._fmt_num(block.get("ajustes_qtd", 0.0), dec=2),
                    liq=self._fmt_num(block.get("saldo_liquido_qtd", 0.0), dec=2),
                )
            )

        top_saida = movements.get("top_saida_30", [])
        if top_saida:
            lines.append("\nItens com maior saida nos ultimos 30 dias:")
            for item in top_saida[:5]:
                lines.append(
                    "- {sku} | {desc} | saida {qtd} | valor {valor}".format(
                        sku=item.get("sku", ""),
                        desc=item.get("descricao", ""),
                        qtd=self._fmt_num(item.get("quantidade", 0.0), dec=2),
                        valor=self._fmt_money(item.get("valor", 0.0)),
                    )
                )

        top_entrada = movements.get("top_entrada_30", [])
        if top_entrada:
            lines.append("\nItens com maior entrada nos ultimos 30 dias:")
            for item in top_entrada[:5]:
                lines.append(
                    "- {sku} | {desc} | entrada {qtd} | valor {valor}".format(
                        sku=item.get("sku", ""),
                        desc=item.get("descricao", ""),
                        qtd=self._fmt_num(item.get("quantidade", 0.0), dec=2),
                        valor=self._fmt_money(item.get("valor", 0.0)),
                    )
                )

        latest = movements.get("latest", {})
        last_ent = latest.get("entrada")
        last_sai = latest.get("saida")
        if last_ent:
            lines.append(
                "\nUltima entrada registrada: {data} | {sku} | qtd {qtd} | valor {valor}".format(
                    data=self._fmt_date(last_ent.get("data")),
                    sku=last_ent.get("sku", ""),
                    qtd=self._fmt_num(last_ent.get("quantidade", 0.0), dec=2),
                    valor=self._fmt_money(last_ent.get("valor", 0.0)),
                )
            )
        if last_sai:
            lines.append(
                "Ultima saida registrada: {data} | {sku} | qtd {qtd} | valor {valor}".format(
                    data=self._fmt_date(last_sai.get("data")),
                    sku=last_sai.get("sku", ""),
                    qtd=self._fmt_num(last_sai.get("quantidade", 0.0), dec=2),
                    valor=self._fmt_money(last_sai.get("valor", 0.0)),
                )
            )

        return "\n".join(lines)

    def _answer_latest_movements(self, ctx: dict, question_norm: str) -> str:
        movements = ctx.get("movements", {})
        latest = movements.get("latest", {})
        last_ent = latest.get("entrada")
        last_sai = latest.get("saida")

        wants_ent = "entrada" in question_norm or "entradas" in question_norm
        wants_sai = "saida" in question_norm or "saidas" in question_norm
        if not wants_ent and not wants_sai:
            wants_ent = True
            wants_sai = True

        matched = ctx.get("inventory", {}).get("matched_items", [])
        by_item = latest.get("by_item", {})
        if matched:
            sku = matched[0].get("sku", "")
            item_moves = by_item.get(sku, {})
            item_ent = item_moves.get("entrada")
            item_sai = item_moves.get("saida")
            lines = [f"Ultimas movimentacoes do item {sku}:"]
            if wants_ent:
                if item_ent:
                    lines.append(
                        "- Ultima entrada: {data} | qtd {qtd} | valor {valor}".format(
                            data=self._fmt_date(item_ent.get("data")),
                            qtd=self._fmt_num(item_ent.get("quantidade", 0.0), dec=2),
                            valor=self._fmt_money(item_ent.get("valor", 0.0)),
                        )
                    )
                else:
                    lines.append("- Ultima entrada: sem registro para este item.")
            if wants_sai:
                if item_sai:
                    lines.append(
                        "- Ultima saida: {data} | qtd {qtd} | valor {valor}".format(
                            data=self._fmt_date(item_sai.get("data")),
                            qtd=self._fmt_num(item_sai.get("quantidade", 0.0), dec=2),
                            valor=self._fmt_money(item_sai.get("valor", 0.0)),
                        )
                    )
                else:
                    lines.append("- Ultima saida: sem registro para este item.")
            return "\n".join(lines)

        lines = ["Ultimas movimentacoes gerais registradas:"]
        if wants_ent:
            if last_ent:
                lines.append(
                    "- Ultima entrada: {data} | {sku} | qtd {qtd} | valor {valor}".format(
                        data=self._fmt_date(last_ent.get("data")),
                        sku=last_ent.get("sku", ""),
                        qtd=self._fmt_num(last_ent.get("quantidade", 0.0), dec=2),
                        valor=self._fmt_money(last_ent.get("valor", 0.0)),
                    )
                )
            else:
                lines.append("- Ultima entrada: sem registro.")
        if wants_sai:
            if last_sai:
                lines.append(
                    "- Ultima saida: {data} | {sku} | qtd {qtd} | valor {valor}".format(
                        data=self._fmt_date(last_sai.get("data")),
                        sku=last_sai.get("sku", ""),
                        qtd=self._fmt_num(last_sai.get("quantidade", 0.0), dec=2),
                        valor=self._fmt_money(last_sai.get("valor", 0.0)),
                    )
                )
            else:
                lines.append("- Ultima saida: sem registro.")
        return "\n".join(lines)

    def _answer_items(self, ctx: dict) -> str:
        inventory = ctx.get("inventory", {})
        matched = inventory.get("matched_items", [])
        lines = []

        if matched:
            lines.append("Itens encontrados para sua pergunta:")
            for item in matched[:8]:
                lines.append(
                    "- {sku} | {desc} | saldo {saldo} | valor {valor} | status {status}".format(
                        sku=item.get("sku", ""),
                        desc=item.get("descricao", ""),
                        saldo=self._fmt_num(item.get("saldo_qtd", 0.0), dec=2),
                        valor=self._fmt_money(item.get("saldo_valor", 0.0)),
                        status=item.get("status", "OK"),
                    )
                )
            return "\n".join(lines)

        top_items = inventory.get("top_items", [])
        if top_items:
            lines.append("Nao encontrei um item especifico na pergunta. Top itens por valor em estoque:")
            for item in top_items[:6]:
                lines.append(
                    "- {sku} | {desc} | saldo {saldo} | valor {valor} | status {status}".format(
                        sku=item.get("sku", ""),
                        desc=item.get("descricao", ""),
                        saldo=self._fmt_num(item.get("saldo_qtd", 0.0), dec=2),
                        valor=self._fmt_money(item.get("saldo_valor", 0.0)),
                        status=item.get("status", "OK"),
                    )
                )
            return "\n".join(lines)

        return "Nao ha dados de itens disponiveis no momento."

    def _answer_alerts(self, ctx: dict) -> str:
        alerts = ctx.get("alerts", {})
        inventory = ctx.get("inventory", {})
        lines = ["Resumo de alertas e riscos:"]
        lines.append(f"- Alertas ativos: **{int(alerts.get('ativos', 0))}**")
        lines.append(f"- Alertas resolvidos: **{int(alerts.get('resolvidos', 0))}**")
        lines.append(f"- Itens em ruptura: **{int(inventory.get('ruptura_count', 0))}**")
        lines.append(f"- Itens abaixo do minimo: **{int(inventory.get('abaixo_min_count', 0))}**")
        return "\n".join(lines)

    def _answer_risk_items(self, ctx: dict) -> str:
        inventory = ctx.get("inventory", {})
        rupture_items = inventory.get("rupture_items", []) or []
        near_zero_items = inventory.get("near_zero_items", []) or []

        lines = [
            "Itens com saldo em zero ou perto de zerar:",
            f"- Em ruptura (saldo zerado): **{len(rupture_items)}**",
            f"- Perto de zerar: **{len(near_zero_items)}**",
        ]

        if rupture_items:
            lines.append("\nEm ruptura:")
            for item in rupture_items[:12]:
                lines.append(
                    "- {sku} | {desc} | saldo {saldo} | minimo {minimo}".format(
                        sku=item.get("sku", ""),
                        desc=item.get("descricao", ""),
                        saldo=self._fmt_num(item.get("saldo_qtd", 0.0), dec=2),
                        minimo=self._fmt_num(item.get("estoque_minimo", 0.0), dec=2),
                    )
                )

        if near_zero_items:
            lines.append("\nPerto de zerar:")
            for item in near_zero_items[:12]:
                lines.append(
                    "- {sku} | {desc} | saldo {saldo} | minimo {minimo}".format(
                        sku=item.get("sku", ""),
                        desc=item.get("descricao", ""),
                        saldo=self._fmt_num(item.get("saldo_qtd", 0.0), dec=2),
                        minimo=self._fmt_num(item.get("estoque_minimo", 0.0), dec=2),
                    )
                )

        if not rupture_items and not near_zero_items:
            lines.append("\nNao ha itens nessa condicao no momento.")

        return "\n".join(lines)

    def _answer_predictive(self, ctx: dict) -> str:
        predictive = ctx.get("predictive")
        if not predictive:
            return (
                "Nao consegui calcular o resumo preditivo agora. "
                "Tente novamente em alguns segundos para atualizar os cenarios."
            )

        horizons = predictive.get("horizontes", [])
        lines = ["Resumo preditivo de estoque:"]
        for row in horizons:
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
            lines.append("\nItens com maior risco de ruptura no horizonte de 30 dias:")
            for item in top_rupt[:5]:
                lines.append(
                    "- {sku} | proj {proj} | demanda {dem}".format(
                        sku=item.get("sku", ""),
                        proj=self._fmt_num(item.get("estoque_projetado", 0.0), dec=2),
                        dem=self._fmt_num(item.get("demanda_prevista", 0.0), dec=2),
                    )
                )

        top_vencer = predictive.get("top_vencer_30", [])
        if top_vencer:
            lines.append("\nItens com risco de vencimento no horizonte de 30 dias:")
            for item in top_vencer[:5]:
                lines.append(
                    "- {sku} | qtd a vencer {qtd} | proj {proj}".format(
                        sku=item.get("sku", ""),
                        qtd=self._fmt_num(item.get("qtd_vencer", 0.0), dec=2),
                        proj=self._fmt_num(item.get("estoque_projetado", 0.0), dec=2),
                    )
                )

        return "\n".join(lines)

    def _answer_general(self, ctx: dict) -> str:
        main = ctx.get("main_kpis", {})
        finance = ctx.get("finance", {})
        inventory = ctx.get("inventory", {})
        ref_date = ctx.get("reference_date", dt.date.today())

        lines = [f"Resumo operacional ({ref_date}):"]
        lines.append(f"- Saldo total: **{self._fmt_num(main.get('saldo', 0.0), dec=2)}** unidades")
        lines.append(f"- Cobertura: **{self._fmt_num(main.get('cobertura_dias', 0.0), dec=1)}** dias")
        lines.append(f"- Valor em estoque: **{self._fmt_money(finance.get('saldo_total_valor', 0.0))}**")
        lines.append(f"- Itens em ruptura: **{int(inventory.get('ruptura_count', 0))}**")
        lines.append("\nVoce pode perguntar por item (SKU/descricao), saldo em dinheiro, movimentacoes ou analise preditiva.")
        return "\n".join(lines)

    def responder(self, question: str, context: dict) -> str:
        q = self._normalize_text(question)

        asks_latest = self._has_any(q, self.LAST_HINTS) and (
            "entrada" in q or "saida" in q or "moviment" in q
        )
        if asks_latest:
            return self._answer_latest_movements(context, q)

        asks_risk_detail = self._has_any(q, self.RISK_HINTS) or (
            "detalh" in q
            and (
                context.get("inventory", {}).get("rupture_items")
                or context.get("inventory", {}).get("near_zero_items")
            )
        )
        if asks_risk_detail:
            return self._answer_risk_items(context)

        if self._has_any(q, self.PREDICTIVE_HINTS):
            return self._answer_predictive(context)

        if self._has_any(q, self.MONEY_HINTS):
            return self._answer_money(context)

        if self._has_any(q, self.MOVEMENT_HINTS):
            return self._answer_movements(context)

        if self._has_any(q, self.ALERT_HINTS):
            return self._answer_alerts(context)

        if self._has_any(q, self.ITEM_HINTS) or context.get("inventory", {}).get("matched_items"):
            return self._answer_items(context)

        return self._answer_general(context)
