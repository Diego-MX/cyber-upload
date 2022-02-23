from azure.identity import ClientSecretCredential
from azure.identity._credentials.default import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from config import PLATFORM_KEYS, ConfigEnviron



class AzureResourcer(): 
    def __init__(self, env_obj: ConfigEnviron): 
        self.env = env_obj.env
        self.config = PLATFORM_KEYS[env_obj.env]
        self.pre_secret = env_obj.get_secret


    def set_credential(self):
        if self.env in ['local', 'databricks']: 
            principal = self.config['service-principal']
            the_creds = ClientSecretCredential(**{k: self.pre_secret(v) 
                    for (k, v) in principal.items()})
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
            not_tuple = not isinstance(val, tuple)
            to_pass = val if not_tuple else self.get_secret(val[1])
            return to_pass
        return {k: pass_val(v) for (k, v) in a_dict.items()}

