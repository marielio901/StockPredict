import streamlit as st


st.title("Informacoes - StockPredict")
st.caption(
    "Plataforma de gestao de estoque com analise operacional, previsao de demanda e assistente de IA."
)

tab_1, tab_2, tab_3, tab_4 = st.tabs(
    ["Visao Geral", "Funcionalidades", "Integracao ERP", "Arquitetura"]
)

with tab_1:
    st.markdown(
        """
### O que e o StockPredict

O **StockPredict** centraliza dados de estoque, movimentacoes e risco operacional
em um unico painel para apoiar decisoes de reposicao e planejamento.

### Para que serve

- Monitorar saude do estoque em tempo real
- Reduzir ruptura e excesso de estoque
- Melhorar previsibilidade de demanda
- Acompanhar saldo fisico e saldo financeiro
- Responder perguntas operacionais via Assistente de IA

### Publico alvo

- Gestao de estoque e suprimentos
- Compras e planejamento
- Operacao logistica
- Lideranca com visao executiva de KPIs
"""
    )

with tab_2:
    st.markdown(
        """
### Modulos do sistema

| Pagina | Descricao |
|--------|-----------|
| Dashboard | Controle Geral + Indicadores de Performance + Indicadores de Armazenagem |
| Estoque | Cadastro de itens, saldos, status e ajuste de estoque minimo |
| Previsao | Cenarios de demanda, simulacao por item e benchmark de modelos |
| Alertas | Ruptura, itens abaixo do minimo, vencendo/vencidos e tratativa |
| Assistente | Perguntas sobre saldo, financeiro, movimentacoes, riscos e preditiva |

### Exemplos de perguntas para o Assistente

- "Qual foi a ultima saida?"
- "Qual o saldo do item CAN-FER-004?"
- "Quais itens estao com saldo em zero ou perto de zerar?"
- "Qual o saldo em dinheiro do estoque?"
- "O que a analise preditiva mostra para o proximo mes?"
"""
    )

with tab_3:
    st.markdown(
        """
### Adaptavel para ERP (SAP, TOTVS e outros)

O projeto foi desenhado para integrar com diferentes fontes de dados de estoque.
Ele **nao e conector oficial homologado**, mas e adaptavel por ETL, API ou visao SQL.

### Cenarios comuns de integracao

- SAP (ex.: Business One, ECC/S4 com camada de integracao)
- TOTVS (ex.: Protheus e outras linhas com acesso a dados)
- Outros ERPs com tabelas de materiais e movimentacoes

### Bancos de dados que podem ser usados

- SQLite (padrao atual)
- PostgreSQL
- SQL Server
- Oracle
- MySQL/MariaDB

### Mapeamento minimo esperado

1. Item/SKU
2. Descricao
3. Categoria
4. Unidade
5. Estoque minimo
6. Movimentacoes (ENTRADA, SAIDA, AJUSTE)
7. Data da movimentacao
8. Custo unitario
"""
    )

with tab_4:
    st.markdown(
        """
### Arquitetura em camadas

- `pages/`: interface Streamlit
- `src/models/`: entidades SQLAlchemy
- `src/repositories/`: acesso aos dados
- `src/services/`: regras de negocio, KPIs, previsao e IA
- `src/db/`: engine, bootstrap e seed

### Modelo de dados principal

- `materiais`
- `movimentos_estoque`
- `locais_estoque`
- `lotes`
- `alertas`
- `modelos_forecast`

### Stack tecnologica

- Python
- Streamlit
- Pandas
- Plotly
- SQLAlchemy
- Prophet
- Requests
- python-dotenv
- OpenPyXL

### Versao

- StockPredict v1.0.0
"""
    )

st.info("SAP e TOTVS sao marcas de seus respectivos proprietarios.")
