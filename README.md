# StockPredict

StockPredict e uma plataforma de gestao de estoque com analise operacional, previsao de demanda e assistente de IA para suporte a decisao.

## O que o projeto e
StockPredict centraliza informacoes de estoque, movimentacoes e risco operacional em um unico painel.
Ele combina visao executiva (KPI), analise preditiva e assistente conversacional conectado aos dados reais.

## Para que o projeto serve
- Monitorar saude do estoque em tempo real
- Reduzir ruptura e excesso de estoque
- Apoiar reposicao com base em consumo historico e previsao
- Acompanhar valor financeiro do estoque e movimentacoes
- Responder perguntas operacionais por chat (saldo, entradas, saidas, riscos e preditiva)

## Principais funcionalidades

### 1) Dashboard (3 subpaginas)
- Controle Geral (visao executiva)
- Indicadores de Performance
- Indicadores de Armazenagem

### 2) Estoque
- Cadastro e manutencao de itens
- Visao consolidada de saldo, minimo, status e valor medio
- Busca por COD_ID e descricao

### 3) Previsao
- Cenarios por horizonte (30, 180, 365 dias)
- Simulacao item a item
- Benchmark de modelos e backtest
- Analise de risco de ruptura e vencimento

### 4) Alertas
- Central de alertas ativos e historico
- Quadro de ruptura e itens abaixo do minimo
- Itens vencendo / vencidos

### 5) Assistente de IA
- Responde sobre saldo de itens, saldo financeiro, movimentacoes e previsao
- Suporta perguntas como:
  - "qual o saldo do item X?"
  - "qual foi a ultima saida?"
  - "quais itens estao em ruptura ou perto de zerar?"
  - "o que a analise preditiva indica para o proximo mes?"
- Usa LLM (Together) com fallback deterministico baseado no banco

## Arquitetura

### Camadas
- `pages/`: interface Streamlit (telas)
- `src/models/`: modelos SQLAlchemy
- `src/repositories/`: acesso a dados
- `src/services/`: regras de negocio, KPIs, previsao, IA
- `src/db/`: engine e seed

### Modelo de dados principal
- `materiais`
- `movimentos_estoque`
- `locais_estoque`
- `lotes`
- `alertas`
- `modelos_forecast`

### Fluxo geral
1. Dados entram no banco (seed ou integracao)
2. Services consolidam KPIs e cenarios
3. Pages renderizam cards, tabelas e graficos
4. Assistente consulta contexto operacional e responde

## Tecnologias
- Python 3.10+
- Streamlit
- Pandas
- Plotly
- SQLAlchemy
- Prophet
- Requests
- python-dotenv
- OpenPyXL

## Estrutura do projeto
- `app.py`: ponto de entrada da aplicacao
- `pages/00_Dashboard.py`: dashboard com subpaginas
- `pages/01_Materiais.py`: estoque
- `pages/02_Previsao.py`: previsao e simulacao
- `pages/03_Alertas.py`: central de alertas
- `pages/05_Assistente.py`: assistente conversacional
- `src/services/`: calculos de KPI, previsao e IA
- `src/repositories/`: acesso ao banco
- `src/models/schemas.py`: entidades principais

## Requisitos
- Python 3.10 ou superior
- Pip
- Ambiente virtual recomendado

## Instalacao
1. Clone o repositorio
2. Crie e ative um ambiente virtual
3. Instale dependencias:

```bash
pip install -r requirements.txt
```

4. Configure variaveis de ambiente (opcional para IA online):

```bash
copy .env.example .env
```

Edite `.env` com sua chave da Together (opcional):
- `TOGETHER_API_KEY`
- `TOGETHER_MODEL`
- `DEBUG`

## Execucao

### Opcao A: seed manual
```bash
python src/db/seed.py
streamlit run app.py
```

### Opcao B: boot automatico
```bash
streamlit run app.py
```
Se o banco estiver vazio, o sistema cria estrutura e popula dados automaticamente.

## Adaptacao para ERP e bancos corporativos (SAP, TOTVS e outros)

StockPredict foi desenhado para ser adaptavel a diferentes fontes de dados de estoque.

### Importante
- O projeto nao e um conector oficial homologado de SAP ou TOTVS.
- Ele pode ser integrado via ETL/API/DB com mapeamento de campos.

### ERPs suportados por adaptacao
- SAP (ex.: SAP Business One, SAP ECC/S4 via camada de integracao)
- TOTVS (ex.: Protheus e outros produtos com acesso a dados)
- Outros ERPs com tabelas de materiais e movimentacoes

### Bancos de dados relacionados a estoque
Como a base usa SQLAlchemy, e possivel adaptar para:
- PostgreSQL
- SQL Server
- Oracle
- MySQL/MariaDB
- SQLite (padrao atual)

### Como adaptar na pratica
1. Definir fonte de dados (API, visao SQL ou arquivo)
2. Mapear campos para o modelo interno:
   - Item/SKU
   - Descricao
   - Categoria
   - Unidade
   - Estoque minimo
   - Movimentacoes (ENTRADA, SAIDA, AJUSTE)
   - Data de movimentacao
   - Custo unitario
3. Popular `materiais` e `movimentos_estoque`
4. Executar dashboards e assistente sobre essa base

### Exemplo de URL de banco (ajuste em `src/config/settings.py`)
```python
DATABASE_URL = "postgresql+psycopg2://user:pass@host:5432/estoque"
# ou
DATABASE_URL = "mssql+pyodbc://user:pass@host:1433/db?driver=ODBC+Driver+17+for+SQL+Server"
```

## Integracao recomendada com ERP
- Camada staging: importar dados do ERP para tabelas staging
- Validacao: padronizar tipos e regras de negocio
- Carga incremental: atualizar movimentos por data/documento
- Observabilidade: logs de carga, erros e reconciliacao

## Boas praticas para producao
- Executar cargas incrementais em janela definida
- Garantir chave unica por movimento (documento + item + data)
- Tratar cancelamentos/estornos na regra de ajuste
- Versionar regras de mapeamento por ERP
- Monitorar performance de consultas e indices

## Limites atuais
- Base padrao local (SQLite)
- Integracao ERP depende de mapeamento no cliente
- Algumas metricas operacionais avancadas (ex.: dock-to-stock por evento WMS real) podem exigir eventos adicionais

## Resumo executivo
StockPredict e um projeto pronto para evoluir de um painel local para um hub de estoque integrado com ERP, mantendo a mesma base de KPIs, previsao e IA operacional.

SAP e TOTVS sao marcas de seus respectivos proprietarios.
