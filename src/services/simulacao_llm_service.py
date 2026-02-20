import datetime

class SimulacaoLLMService:
    """Serviço de simulação que gera respostas inteligentes baseadas nos KPIs reais, sem custo de API."""

    def responder(self, pergunta: str, kpis: dict) -> str:
        pergunta_lower = pergunta.lower()
        saldo = kpis.get('saldo', 0)
        cobertura = kpis.get('cobertura_dias', 0)
        entradas = kpis.get('entradas', 0)
        saidas = kpis.get('saidas', 0)
        hoje = datetime.date.today().strftime("%d/%m/%Y")

        # Análise de risco
        risco = "BAIXO"
        if cobertura < 7:
            risco = "CRÍTICO"
        elif cobertura < 15:
            risco = "ALTO"
        elif cobertura < 30:
            risco = "MODERADO"

        # Respostas contextuais baseadas na pergunta
        if any(w in pergunta_lower for w in ["ruptura", "risco", "falta", "acabar", "zerar"]):
            return (
                f"📊 **Análise de Risco de Ruptura** ({hoje})\n\n"
                f"Com base nos dados atuais:\n"
                f"- **Saldo total**: {saldo:.0f} unidades\n"
                f"- **Cobertura**: {cobertura:.1f} dias\n"
                f"- **Nível de risco**: {risco}\n\n"
                f"{'⚠️ **ATENÇÃO**: A cobertura está abaixo de 15 dias! Recomendo gerar ordens de reposição imediatas.' if cobertura < 15 else '✅ A cobertura está saudável no momento.'}\n\n"
                f"**Recomendação**: Verifique a página de Alertas para ver itens específicos em risco.\n\n"
                f"_Fontes: movimentos_estoque, alertas_"
            )

        elif any(w in pergunta_lower for w in ["consumo", "média", "media", "gasto", "uso"]):
            consumo_diario = saidas / 30 if saidas > 0 else 0
            return (
                f"📈 **Análise de Consumo** ({hoje})\n\n"
                f"- **Saídas totais (período)**: {saidas:.0f} unidades\n"
                f"- **Consumo médio diário**: {consumo_diario:.1f} un/dia\n"
                f"- **Consumo médio semanal**: {consumo_diario * 7:.1f} un/semana\n"
                f"- **Consumo médio mensal**: {consumo_diario * 30:.1f} un/mês\n\n"
                f"**Tendência**: {'📉 Consumo elevado — revise as projeções.' if consumo_diario > 10 else '📊 Consumo dentro da normalidade.'}\n\n"
                f"💡 Use a página **Previsão Prophet** para projetar a demanda futura por SKU.\n\n"
                f"_Fontes: movimentos_estoque_"
            )

        elif any(w in pergunta_lower for w in ["reposição", "reposicao", "comprar", "pedir", "ordem"]):
            return (
                f"🛒 **Plano de Reposição** ({hoje})\n\n"
                f"**Situação atual:**\n"
                f"- Saldo: {saldo:.0f} un | Cobertura: {cobertura:.1f} dias\n"
                f"- Entradas recentes: {entradas:.0f} un | Saídas: {saidas:.0f} un\n\n"
                f"**Recomendação:**\n"
                f"1. Priorize os itens com cobertura < 7 dias (veja Alertas)\n"
                f"2. Considere o lead time de cada fornecedor\n"
                f"3. Use a Previsão Prophet para estimar demanda no lead time\n"
                f"4. Quantidade sugerida = demanda_prevista_leadtime - saldo_atual\n\n"
                f"_Fontes: movimentos_estoque, materiais, alertas_"
            )

        elif any(w in pergunta_lower for w in ["validade", "vencimento", "vencer", "lote", "expirar"]):
            return (
                f"🏷️ **Controle de Validade** ({hoje})\n\n"
                f"Para verificar lotes vencendo:\n"
                f"1. Acesse a página **Alertas**\n"
                f"2. Filtre por status VENCENDO ou VENCIDO\n"
                f"3. Priorize FEFO (First Expired, First Out)\n\n"
                f"**Ações recomendadas para lotes vencendo:**\n"
                f"- Transferir para área de venda rápida\n"
                f"- Considerar promoções ou descarte controlado\n"
                f"- Registrar movimento de AJUSTE/SAÍDA para baixa\n\n"
                f"_Fontes: lotes, movimentos_estoque_"
            )

        elif any(w in pergunta_lower for w in ["kpi", "indicador", "resumo", "status", "geral", "como está"]):
            return (
                f"📋 **Resumo Executivo** ({hoje})\n\n"
                f"| Indicador | Valor |\n"
                f"|-----------|-------|\n"
                f"| Entradas | {entradas:.0f} un |\n"
                f"| Saídas | {saidas:.0f} un |\n"
                f"| Saldo Total | {saldo:.0f} un |\n"
                f"| Cobertura | {cobertura:.1f} dias |\n"
                f"| Nível de Risco | {risco} |\n\n"
                f"{'🔴 Ação necessária: revise os alertas urgentes!' if risco in ['CRÍTICO', 'ALTO'] else '🟢 Operação dentro da normalidade.'}\n\n"
                f"_Fontes: movimentos_estoque, materiais, alertas_"
            )

        else:
            return (
                f"🤖 **Assistente StockPredict** ({hoje})\n\n"
                f"Aqui está um resumo rápido:\n"
                f"- Saldo: **{saldo:.0f}** un | Cobertura: **{cobertura:.1f}** dias | Risco: **{risco}**\n\n"
                f"Posso te ajudar com:\n"
                f"- 📊 \"Qual o risco de ruptura?\"\n"
                f"- 📈 \"Qual o consumo médio?\"\n"
                f"- 🛒 \"Sugira um plano de reposição\"\n"
                f"- 🏷️ \"Quais lotes estão vencendo?\"\n"
                f"- 📋 \"Como está meu estoque?\"\n\n"
                f"_Modo: Simulação (sem custo de API) | Fontes: dados internos_"
            )
