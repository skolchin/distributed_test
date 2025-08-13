from pydantic_settings import BaseSettings, SettingsConfigDict

class Options(BaseSettings):
    """ Backend options """

    model_config = SettingsConfigDict(env_file='.env', extra='allow')

    cluster_address: str
    """ Cluster IP address or domain name """

    sqlite_file_name: str = "./data/db.db"
    """ Database name"""

    max_inline_result_size: int = 512
    """ Maximum size of result to be returned inline, in bytes """

    default_result_ttl: int = 300
    """ Default TTL for result to be kept """

    default_timeout: int = 600
    """ Default ray request timeout """

    result_storage_uri: str | None = None
    """ URI of result Redis storage or None """