import os
from pathlib import Path
from dotenv import load_dotenv

# Carrega .env do diretório do projeto explicitamente
_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(_env_path, override=True)

class Settings:
    # App
    APP_NAME = "StockPredict"
    VERSION = "1.0.0"
    DEBUG = os.getenv("DEBUG", "False").lower() == "true"
    
    # Database
    DB_NAME = "stockpredict.db"
    DATABASE_URL = f"sqlite:///{DB_NAME}"
    
    # API Keys
    TOGETHER_API_KEY = os.getenv("TOGETHER_API_KEY", "")
    TOGETHER_MODEL = os.getenv("TOGETHER_MODEL", "meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo")
    
    # Business Logic
    DEFAULT_LEAD_TIME_DAYS = 7
    ALERT_LOOKAHEAD_DAYS = 30

    # Prophet defaults
    PROPHET_HORIZON_DAYS = 30

settings = Settings()

# Debug
print(f"[Settings] API Key loaded: {'YES' if settings.TOGETHER_API_KEY else 'NO'} | Model: {settings.TOGETHER_MODEL}")
print(f"[Settings] Key starts with: {settings.TOGETHER_API_KEY[:8]}...")
