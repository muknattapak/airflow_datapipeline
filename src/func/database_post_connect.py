from settings.configuration import Configuration
from sqlalchemy import create_engine, exc


class DatabasePostgConnect:
    def __init__(self) -> None:
        config = Configuration().getConfig()
        url = config.get("POSTGRES_DATABASE", "URL")
        dbName = config.get("POSTGRES_DATABASE", "DB")
        try:
            conn = create_engine(url, connect_args={'connect_timeout': 10})
            self.conn = conn
            # print(dbName, 'database connected')
        except exc.SQLAlchemyError as e:
            print(type(e))
            print(dbName, "Server not available")

    def getPostgDB(self):
        return self.conn
