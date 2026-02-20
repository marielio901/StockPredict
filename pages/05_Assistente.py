import datetime as dt

import streamlit as st

from src.db.engine import get_db
from src.services.assistant_context_service import AssistantContextService
from src.services.assistant_fallback_service import AssistantFallbackService
from src.services.llm_service import LLMService


SYSTEM_INSTRUCTIONS = """
Voce e o Assistente Operacional do StockPredict.
Responda sempre em portugues do Brasil e seja objetivo.

Regras obrigatorias:
1. Use somente os dados do CONTEXTO OPERACIONAL REAL fornecido.
2. Quando o usuario perguntar saldo de item, retorne SKU, descricao, saldo em quantidade, valor estimado e status.
3. Quando o usuario perguntar saldo financeiro, responda com valores em reais do contexto.
4. Quando o usuario perguntar ultima entrada ou ultima saida, responda com data, SKU, quantidade e valor, usando o bloco de ultimas movimentacoes do contexto.
5. Quando o usuario perguntar movimentacoes, use os blocos de 7, 30 e 90 dias do contexto.
6. Quando o usuario perguntar itens em ruptura, saldo zerado ou perto de zerar, liste os SKUs com saldo e estoque minimo.
7. Quando o usuario perguntar preditiva/previsao, use a secao de resumo preditivo do contexto.
8. Se algum dado nao existir, diga claramente que nao esta disponivel e sugira a melhor pergunta alternativa.
9. Nunca invente numeros.
""".strip()


def llm_response_failed(response: str) -> bool:
    text = str(response or "").strip().lower()
    if not text:
        return True

    failure_marks = [
        "erro: api key",
        "nenhum modelo gratuito",
        "erro ao consultar",
        "nao foi possivel consultar",
    ]
    return any(mark in text for mark in failure_marks)


def should_force_risk_detail(prompt: str) -> bool:
    prompt_norm = AssistantFallbackService._normalize_text(prompt)
    risk_marks = ["ruptura", "zerar", "zero", "perto de zerar", "abaixo do minimo", "falta"]
    detail_marks = ["detalhe", "detalhes", "lista", "quero detalhes"]

    if any(mark in prompt_norm for mark in risk_marks):
        return True

    if any(mark in prompt_norm for mark in detail_marks):
        recent_user_msgs = [
            AssistantFallbackService._normalize_text(m.get("content", ""))
            for m in st.session_state.get("messages", [])
            if m.get("role") == "user"
        ]
        for prev in reversed(recent_user_msgs[-4:]):
            if any(mark in prev for mark in risk_marks):
                return True
    return False


def init_chat():
    if "messages" not in st.session_state:
        st.session_state["messages"] = [
            {
                "role": "assistant",
                "content": (
                    "Ola! Posso responder perguntas sobre saldo de itens, saldo financeiro, "
                    "movimentacoes e analise preditiva do estoque."
                ),
            }
        ]


def render_sidebar_controls():
    with st.sidebar:
        st.caption("Assistente conectado aos dados reais do estoque.")
        if st.button("Limpar Conversa"):
            st.session_state["messages"] = [
                {
                    "role": "assistant",
                    "content": "Conversa limpa. Pode perguntar sobre estoque, financeiro, movimentacoes ou preditiva.",
                }
            ]
            st.rerun()


def build_context(prompt: str):
    db_gen = get_db()
    db = next(db_gen)
    try:
        context_service = AssistantContextService(db)
        context_data = context_service.build_context(prompt)
        context_str = context_service.to_prompt_context(context_data)
    finally:
        db_gen.close()

    return context_data, context_str


def build_llm_messages(prompt: str, context_str: str):
    recent_history = []
    for msg in st.session_state.get("messages", []):
        if msg.get("role") not in {"user", "assistant"}:
            continue
        recent_history.append({"role": msg["role"], "content": msg["content"]})

    if recent_history and recent_history[-1]["role"] == "user" and recent_history[-1]["content"] == prompt:
        recent_history = recent_history[:-1]

    recent_history = recent_history[-6:]

    system_msg = {
        "role": "system",
        "content": f"{SYSTEM_INSTRUCTIONS}\n\nCONTEXTO OPERACIONAL REAL:\n{context_str}",
    }

    return [system_msg] + recent_history + [{"role": "user", "content": prompt}]


def generate_answer(prompt: str) -> str:
    try:
        context_data, context_str = build_context(prompt)
    except Exception as exc:
        fallback = AssistantFallbackService()
        return fallback.responder(
            prompt,
            {
                "reference_date": dt.date.today(),
                "main_kpis": {},
                "finance": {},
                "inventory": {"matched_items": [], "top_items": []},
                "alerts": {},
                "movements": {},
                "predictive": None,
                "error": str(exc),
            },
        )

    prompt_norm = AssistantFallbackService._normalize_text(prompt)
    asks_latest = ("ultima" in prompt_norm or "ultimo" in prompt_norm or "mais recente" in prompt_norm) and (
        "entrada" in prompt_norm or "saida" in prompt_norm or "moviment" in prompt_norm
    )
    if asks_latest:
        fallback = AssistantFallbackService()
        return fallback.responder(prompt, context_data)

    if should_force_risk_detail(prompt):
        fallback = AssistantFallbackService()
        return fallback.responder(prompt, context_data)

    llm = LLMService()
    llm_messages = build_llm_messages(prompt, context_str)

    with st.spinner("Consultando assistente..."):
        try:
            llm_response = llm.query(llm_messages, temperature=0.2)
        except Exception:
            llm_response = ""

    if llm_response_failed(llm_response):
        fallback = AssistantFallbackService()
        return fallback.responder(prompt, context_data)

    return llm_response


st.title("Assistente de Estoque Inteligente")
init_chat()
render_sidebar_controls()

for msg in st.session_state.messages:
    st.chat_message(msg["role"]).write(msg["content"])

if prompt := st.chat_input("Digite sua pergunta..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    st.chat_message("user").write(prompt)

    answer = generate_answer(prompt)

    st.session_state.messages.append({"role": "assistant", "content": answer})
    st.chat_message("assistant").write(answer)
