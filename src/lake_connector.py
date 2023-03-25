from pyodbc import connect as db_connect

from config import ConfigEnviron, DBKS_KEYS


class LakehouseConnect():
    def __init__(self, config_env: ConfigEnviron):
        self.base_env = config_env
        self.config = DBKS_KEYS.get(config_env.env, DBKS_KEYS['dev'])

    def get_connection(self):
        the_keys = self.config['connect'].get(self.base_env.server, 'wap')
        conn_params = self.base_env.call_dict(the_keys)
        conn_str = ";".join( f"{k}={v}" for (k,v) in conn_params.items())
        return db_connect(conn_str, autocommit=True)
