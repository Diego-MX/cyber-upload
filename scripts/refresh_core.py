# Diego Villamil, CDMX
# 6 de septiembre de 2021

import os, sys
from datetime import datetime as dt
from pyarrow import feather

from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

import src.core_banking as core                 
from dotenv import load_dotenv
from config import SITE, ENV_KEYS
load_dotenv(override=True)

#%% Obtener los nuevos préstamos. 

in_dbks = 'ipykernel' not in sys.modules
deposit_how = 'feather' if in_dbks else 'delta'  # blob, feather


hour_str = dt.now().strftime("%Y%m%d_%H%M")

loans_df   = core.get_sap_loans("en_cx")
persons_df = core.get_person_set()


if deposit_how in ["blob", "feather"]: 
    # Se guardan temporalmente los archivos. 
    local_dir   = SITE/"data/cache_tables"

    feather.write_feather(loans_df, local_dir/"loan-contracts.feather") 
    feather.write_feather(persons_df, local_dir/"person-set.feather") 


if deposit_how in ["blob", "delta"]: 
    # Se leen las credenciales
    az_identity  = "service-principal" # "managed" # "service-principal" # "local" # 
    access_vault = az_identity in ["service-principal", "managed"]

    azure_keys = ENV_KEYS["platform"]

    if  az_identity == "managed":
        credential = DefaultAzureCredential() 

    elif az_identity == "service-principal": 
        credential = ClientSecretCredential(**{k: os.getenv(v) 
            for (k, v) in azure_keys["service-principal"].items() })

    elif az_identity == "local": 
        credential = ClientSecretCredential(**{k: os.getenv(v) 
            for (k, v) in azure_keys["service-principal"].items() })


if deposit_how == "blob": 
    # Se leen locales y suben a la dirección propuesta. 
    storage_dir = "ops/core-banking/batch-updates"
    
    blob_container = BlobServiceClient(azure_keys["storage"]["url"], credential)

    for time_tag in ["current", hour_str]:
        for a_tbl in ["loan-contracts", "person-set"]:
        
            blob_path = f"{storage_dir}/{a_tbl}/{time_tag}.feather"
            the_blob  = blob_container.get_blob_client(container="bronze", blob=blob_path)
            with open(local_dir/f"{a_tbl}.feather", "rb") as _f: 
                the_blob.upload_blob(_f, overwrite=True)


if deposit_how == "delta": 
    storage_dir = "ops/core-banking/batch-updates/delta"

    persons_df.to_delta(f"{storage_dir}/current.delta")   
    loans_df.to_delta(f"{storage_dir}/current.delta")
    persons_df.to_delta(f"{storage_dir}/{hour_str}.delta")   
    loans_df.to_delta(f"{storage_dir}/{hour_str}.delta")

