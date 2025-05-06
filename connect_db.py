from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv
import os

load_dotenv()

class CassandraConnection:
    _instance = None
    _cluster = None 

    def __new__(cls):
        if cls._instance is None:
            ASTRA_DB_APPLICATION_TOKEN = os.getenv('ASTRA_DB_APPLICATION_TOKEN')
            secure_connect_bundle = os.getenv('SECURE_CONNECT_BUNDLE')
            auth_provider = PlainTextAuthProvider(username='token', password=ASTRA_DB_APPLICATION_TOKEN)
            cls._cluster = Cluster(cloud={'secure_connect_bundle': secure_connect_bundle}, auth_provider=auth_provider)
            cls._instance = cls._cluster.connect()
            cls._instance.set_keyspace('taxi') 
        return cls._instance

    @classmethod
    def shutdown(cls):
        if cls._cluster:
            cls._cluster.shutdown()
