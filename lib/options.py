from pydantic_settings import BaseSettings, SettingsConfigDict

class BackendOptions(BaseSettings):
    """ Backend options """

    model_config = SettingsConfigDict(env_file='.env', extra='allow')

    cluster_address: str
    """ Cluster IP address or domain name """

    sqlite_file_name: str = "./data/db.db"
    """ Database name"""

