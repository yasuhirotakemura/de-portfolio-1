import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def is_enabled(env_var_name: str) -> bool:
    val = os.getenv(env_var_name, "false").lower()

    if val == "true":
        return True

    return False


def get_settings():
    load_dotenv()

    app_env = os.getenv("APP_ENV")
    project_id = os.getenv("PROJECT_ID")

    if not app_env:
        raise ValueError("環境変数 APP_ENV が設定されていません。")

    if not project_id:
        raise ValueError("環境変数 PROJECT_ID が設定されていません。")

    return (
        app_env,
        project_id,
    )
