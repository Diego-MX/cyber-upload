from azure.keyvault.secrets import SecretClient

from config import PLATFORM_KEYS, ConfigEnviron


class AzureResourcer(): 
    def __init__(self, env_obj: ConfigEnviron): 
        self.env = env_obj
        self.config = PLATFORM_KEYS[env_obj.env]
        self.set_secretters()
    
    def set_vault(self):
        vault_url = self.config['key-vault']['url']
        the_creds = self.env.credential
        self.vault = SecretClient(vault_url=vault_url, credential=the_creds)
    
    def set_secretters(self): 
        if not hasattr(self, 'vault'): 
            self.set_vault()
        
        get_secret = lambda k: self.vault.get_secret(k).value
        
        is_tuple  = lambda x: isinstance(x, tuple)
        secret_2  = lambda v_ish: get_secret(v_ish[1]) if is_tuple(v_ish) else v_ish 
        call_dict = lambda d: {k:secret_2(v) for k, v in d.items()} 
        
        self.get_secret = get_secret 
        self.call_dict  = call_dict
        



