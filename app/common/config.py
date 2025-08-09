from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    app_env: str = Field(default="local")

    # MySQL
    mysql_host: str = Field(default="localhost")
    mysql_port: int = Field(default=3306)
    mysql_db: str = Field(default="blogs")
    mysql_user: str = Field(default="bloguser")
    mysql_password: str = Field(default="blogpass")
    mysql_pool_size: int = Field(default=50)
    mysql_max_overflow: int = Field(default=50)

    # Redis
    redis_url: str = Field(default="redis://localhost:6379/0")

    # gRPC
    dataservice_host: str = Field(default="localhost")
    dataservice_port: int = Field(default=50051)
    grpc_port: int = Field(default=50051)

    # Streams / batching
    stream_maxlen: int = Field(default=200_000)
    consumer_group: str = Field(default="blog_group")
    consumer_name: str = Field(default="worker-1")

    batch_max_count: int = Field(default=1000)
    batch_max_age_ms: int = Field(default=300)
    batch_max_bytes: int = Field(default=2_097_152)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


settings = Settings() 