from azure.identity import ClientSecretCredential
from azure.identity._credentials.default import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from config import PLATFORM_KEYS, ConfigEnviron



class AzureResourcer(): 
    def __init__(self, env, env_obj: ConfigEnviron): 
        self.env = env
        self.config = PLATFORM_KEYS[env]
        self.pre_secret = env_obj.get_secret
        self.pre_dict = env_obj.call_dict


    def set_credential(self):
        if self.env in ['local', 'dbks']: 
            principal_keys = self.pre_dict(self.config['service-principal'])
            the_creds = ClientSecretCredential(**principal_keys)
        elif self.env in ['dev', 'qas']: 
            the_creds = DefaultAzureCredential()
        self.credential = the_creds


    def set_vault(self):
        if not hasattr(self, 'credential'): 
            self.set_credential()
        vault_url = self.config['key-vault']['url']
        self.vault = SecretClient(vault_url=vault_url, credential=self.credential)
    

    def get_secret(self, key): 
        if not hasattr(self, 'vault'): 
            self.set_vault()
        the_secret = self.vault.get_secret(key).value
        return the_secret


    def call_dict(self, a_dict: dict):         
        def pass_val(val): 
            is_tuple = isinstance(val, tuple)
            to_pass = self.get_secret(val[1]) if is_tuple else val
            return to_pass
        return {k: pass_val(v) for (k, v) in a_dict.items()}

