import os
from dotenv import load_dotenv
load_dotenv()

class Settings:
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gpt-4o")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///pipelines.db")
    MAX_PIPELINE_STEPS: int = int(os.getenv("MAX_PIPELINE_STEPS", "10"))
    EXECUTION_TIMEOUT_SECS: int = int(os.getenv("EXECUTION_TIMEOUT_SECS", "300"))
    SANDBOX_ENABLED: bool = os.getenv("SANDBOX_ENABLED", "true").lower() == "true"
    TEMPERATURE: float = float(os.getenv("TEMPERATURE", "0.2"))
    APP_HOST: str = os.getenv("APP_HOST", "0.0.0.0")
    APP_PORT: int = int(os.getenv("APP_PORT", "8000"))

settings = Settings()
