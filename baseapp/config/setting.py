import os
from typing import ClassVar
from pydantic_settings import SettingsConfigDict, BaseSettings


class Settings(BaseSettings):
    # common
    app_env:str
    host:str
    port:int
    domain:str
    
    # jwt
    jwt_secret_key:str
    jwt_algorithm:str
    jwt_access_expired_in:int
    jwt_refresh_expired_in:int

    # api credential
    api_cipher_key:str
    api_key_expired_in:int

    # mongodb
    mongodb_host: str
    mongodb_port: int
    mongodb_user: str
    mongodb_pass: str
    mongodb_db: str

    # clickhouse
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_pass: str
    clickhouse_db: str
    clickhouse_secure: bool = False
    clickhouse_verify: bool = False
    
    # redis
    redis_host: str
    redis_port: int
    redis_max_connections: int

    # rabbit mq
    rabbitmq_host: str
    rabbitmq_port: int

    # Minio
    minio_host: str
    minio_port: int
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool = False
    minio_bucket: str
    minio_verify: bool = True

    # smtp
    smtp_host: str
    smtp_port: int
    smtp_username: str
    smtp_password: str

    file_location: str

    # google
    google_api_key: str
    google_client_id: str
    google_client_secret: str
    google_redirect_uri: str

    # Determine the env file based on the ENV environment variable
    env_file: ClassVar[str] = (
        # print(os.getenv('ENV'))
        os.path.join(
            os.path.dirname(os.path.dirname(__file__)), f".env.{os.getenv('ENV')}"
        )
        if os.getenv("ENV")
        else ".env"
    )
    print(env_file)
    model_config = SettingsConfigDict(env_file=env_file, extra="ignore")

# @lru_cache()
def get_settings():
    # logging.info(f"get_settings: {os.getenv('ENV')}")
    return Settings()
