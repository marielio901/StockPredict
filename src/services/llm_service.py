import requests
import logging
from src.config.settings import settings

logger = logging.getLogger("StockPredict")

# Modelos gratuitos disponíveis na Together AI
FREE_MODELS = [
    "meta-llama/Llama-3.3-70B-Instruct-Turbo-Free",
    "deepseek-ai/DeepSeek-R1-Distill-Llama-70B-free",
    "Qwen/Qwen2.5-72B-Instruct-Turbo",
]

class LLMService:
    def __init__(self):
        self.api_key = settings.TOGETHER_API_KEY
        self.model = settings.TOGETHER_MODEL
        self.base_url = "https://api.together.ai/v1/chat/completions"

    def query(self, messages, temperature=0.7):
        if not self.api_key:
            return "Erro: API Key não configurada. Configure TOGETHER_API_KEY no .env"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # Tenta o modelo configurado e depois fallback para outros free
        models_to_try = [self.model] + [m for m in FREE_MODELS if m != self.model]
        
        for model in models_to_try:
            payload = {
                "model": model,
                "messages": messages,
                "max_tokens": 1024,
                "temperature": temperature,
                "top_p": 0.7,
                "top_k": 50,
                "repetition_penalty": 1
            }
            
            logger.info(f"LLM tentando modelo: {model}")
            
            try:
                response = requests.post(
                    self.base_url, 
                    json=payload, 
                    headers=headers, 
                    timeout=30
                )
                
                if response.ok:
                    data = response.json()
                    reply = data['choices'][0]['message']['content']
                    logger.info(f"LLM OK com modelo: {model} | Resposta: {len(reply)} chars")
                    return reply
                else:
                    error_body = response.text
                    logger.warning(f"Modelo {model} falhou ({response.status_code}): {error_body[:200]}")
                    continue  # Tenta próximo modelo
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout no modelo {model}")
                continue
            except Exception as e:
                logger.error(f"Erro no modelo {model}: {str(e)}")
                continue
        
        return "Nenhum modelo gratuito disponível no momento. Use o modo Simulação (Demo)."
