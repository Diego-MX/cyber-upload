from config import VaultSetter, DBKS_KEYS
from pyodbc import connect as db_connect


class LakehouseConnect():
    def __init__(self, env_type, a_secretter: VaultSetter): 
        self.env = env_type
        self.get_secret = a_secretter.get_secret
        self.call_dict = a_secretter.call_dict
        self.config = DBKS_KEYS.get(env_type, DBKS_KEYS['dev'])

    def get_connection(self): 
        the_keys = self.config['connect'].get(self.env, 'wap')
        conn_params = self.call_dict(the_keys)
        conn_str = ";".join( f"{k}={v}" for (k,v) in conn_params.items())
        return db_connect(conn_str, autocommit=True)
