from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

DOTENVPRIVATE = Path(__file__).parent.parent / ".env.private"


class Configuracion(BaseSettings):
    """Lee y valida las variables de configuración de los archivos .env."""

    model_config = SettingsConfigDict(env_file=DOTENVPRIVATE)

    teradata_host: str = "teradata.suranet.com"
    teradata_user: str = Field(alias="TERADATA_USER")
    teradata_password: str = Field(alias="TERADATA_PASSWORD")


configuracion = Configuracion()
