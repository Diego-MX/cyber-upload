from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from os import remove
from pathlib import Path

from config import PLATFORM_KEYS, ConfigEnviron


class AzureResourcer(): 
    def __init__(self, env_obj: ConfigEnviron): 
        self.env = env_obj
        self.config = PLATFORM_KEYS[env_obj.env]
        self.set_secretters()
    
    
    def set_secretters(self): 
        if not hasattr(self, 'vault'): 
            self.set_vault()
        
        get_secret = lambda k: self.vault.get_secret(k).value
        is_tuple   = lambda x: isinstance(x, tuple)
        secret_2   = lambda v_ish: get_secret(v_ish[1]) if is_tuple(v_ish) else v_ish 
        call_dict  = lambda d: {k:secret_2(v) for k, v in d.items()} 
        
        self.get_secret = get_secret 
        self.call_dict  = call_dict
   
    
    def set_vault(self):
        vault_url = self.config['key-vault']['url']
        the_creds = self.env.credential
        the_vault =  SecretClient(vault_url=vault_url, credential=the_creds)
        to_secret_or_not = the_vault.list_properties_of_secrets()
        next(to_secret_or_not)
        self.vault = the_vault
        
    
    def get_storage(self, account=None): 
        if ('storage' in self.config) and ('name' in self.config['storage']):
            return self.config['storage']['name']
    
    
    def set_dbks_permissions(self, blob_key=None, gen_value='gen2'):
        # Assume account corresponding to BLOB_KEY is GEN2.  
        # and permissions are unlocked directly via CONFIG.SETUP_KEYS
        
        sp_items = self.env.config['service-principal']
        secretter = self.env.get_secret
        gen_value = 'gen2'
        
        tenant_id = secretter(sp_items['tenant_id'][1]) 
        oauth2_endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        if gen_value == 'gen2':
            pre_confs = {
                f"fs.azure.account.auth.type.{blob_key}.dfs.core.windows.net"           : 'OAuth',
                f"fs.azure.account.oauth.provider.type.{blob_key}.dfs.core.windows.net" : "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                f"fs.azure.account.oauth2.client.endpoint.{blob_key}.dfs.core.windows.net" : oauth2_endpoint,
                f"fs.azure.account.oauth2.client.id.{blob_key}.dfs.core.windows.net"    : sp_items['client_id'],
                f"fs.azure.account.oauth2.client.secret.{blob_key}.dfs.core.windows.net": sp_items['client_secret']}
        elif gen_value == 'gen1': 
            pre_confs = {
                f"fs.adl.oauth2.access.token.provider.type"    : 'ClientCredential', 
                f"fs.adl.account.{blob_key}.oauth2.client.id"  : sp_items['client_id'],     # aplication-id
                f"fs.adl.account.{blob_key}.oauth2.credential" : sp_items['client_secret'], # service-credential
                f"fs.adl.account.{blob_key}.oauth2.refresh.url": oauth2_endpoint}
        elif gen_value == 'v2': 
            pre_confs = {f"fs.azure.account.key.{blob_key}.blob.core.windows.net" : sp_items['sas_string']}
        
        for a_conf, its_val in self.env.call_dict(pre_confs).items():
            print(f"{a_conf} = {its_val}")
            self.env.spark.conf.set(a_conf, its_val)
    

    def upload_storage_blob(self, file, blob, container=None, account=None, overwrite=False, verbose=0): 
        if account is None: 
            account = self.get_storage()
        
        the_url = f"https://{account}.blob.core.windows.net"
        b_service = BlobServiceClient(the_url, credential=self.env.credential)

        the_blob  = b_service.get_blob_client(container, blob)
        with open(file, 'rb') as _b:
            the_blob.upload_blob(_b, overwrite=overwrite)
            if verbose: 
                print(f"Blob uploaded: {account}, {container};\n\t{blob}")
        
        
    def download_storage_blob(self, file, blob, container, account=None, verbose=0): 
        if account is None: 
            account = self.get_storage()
        
        the_url = f"https://{account}.blob.core.windows.net"
        b_service = BlobServiceClient(the_url, credential=self.env.credential)

        if Path(file).is_file(): 
            remove(file)

        the_blob = b_service.get_blob_client(container, blob)
        with open(file, 'wb') as _b: 
            b_data = the_blob.download_blob()
            b_data.readinto(_b)
    

    