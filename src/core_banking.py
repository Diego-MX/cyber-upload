# Diego Villamil, EPIC
# CDMX, 4 de noviembre de 2021
# Copy del archivo en el repo DATA-SAP-EVENTS. 
# No lo hemos importado, sólo copiado-pegado. 

import os, sys, pandas as pd, re

import json
from datetime import datetime as dt
from requests import get as rq_get, post
from requests.auth import HTTPBasicAuth
from urllib.parse import unquote
from operator import itemgetter

from azure.keyvault.secrets import SecretClient
from azure.identity import ClientSecretCredential
from azure.identity._credentials.default import DefaultAzureCredential

from src.utilities import tools
from dotenv import load_dotenv
from config import SITE, URLS, ENV_KEYS, AZ_IDENTITY

load_dotenv(override=True)

urls     = URLS["sap-calls"]["qa"]
AUTH_URL = urls["auth"]
APIS_URL = urls["apis"]

# Access to Auth. 
access_vault = AZ_IDENTITY in ["service-principal", "managed"]

core_keys    = "qa-vault" if access_vault else "qa"
core_access  = ENV_KEYS["core"][core_keys]  

azure_keys   = ENV_KEYS["platform"]


if  AZ_IDENTITY == "managed":
    if "ipykernel" in sys.modules:
        secret_by_key = lambda a_key: dbutils.secrets.get(scope="kv-resource-access-dbks", key=a_key)

        az_creds = ClientSecretCredential(**{k: secret_by_key(v) 
            for (k, v) in azure_keys["service-principal"].items() })
    else:
        az_creds = DefaultAzureCredential()

    vault_client = SecretClient(vault_url=azure_keys["key-vault"]["url"], 
        credential=az_creds)

    get_secret = lambda name: vault_client.get_secret(name).value

elif AZ_IDENTITY == "service-principal": 
    az_creds = ClientSecretCredential(**{k: os.getenv(v) 
        for (k, v) in azure_keys["service-principal"].items() })
    vault_client = SecretClient(vault_url=azure_keys["key-vault"]["url"], 
            credential=az_creds)
    get_secret = lambda name: vault_client.get_secret(name).value

elif AZ_IDENTITY == "local": 
    get_secret = os.getenv    


KEY      = get_secret(core_access["key"])
USER     = get_secret(core_access["user"])
PASS     = get_secret(core_access["pass"])
SECRET   = get_secret(core_access["secret"])

sap_token_file = SITE/"refs/config/SAP_token.json"

# GET_SAP_EVENTS
event_types = ["person-set", "current-account", "loan-contract", "transaction-set"]



class CoreAPI():
    pass


event_type = event_types[0]
def get_event_objects(event_type): 
    the_token  = get_token(sap_token_file)
    the_hdrs   = get_headers("apis")
    event_call = tools.str_snake_to_camel(event_type, True)
    
    the_resp   = rq_get(f"{APIS_URL}/v1/{event_call}", 
        auth=tools.BearerAuth(the_token), headers=the_hdrs)
    
    return the_resp


def get_sap_loans(attrs_indicator=None):
    the_token    = get_token(sap_token_file, False, "header")
    
    the_hdrs     = get_headers("apis", "headers")
    select_attrs = attributes_from_column(attrs_indicator)
    the_params   = { "$select" : ",".join(select_attrs) }

    the_resp     = rq_get(f"{APIS_URL}/v1/lacovr/ContractSet", 
        auth=tools.BearerAuth(the_token), headers=the_hdrs, params=the_params)

    loans_ls     = the_resp.json()["d"]["results"]  # [metadata : [id, uri, type], borrowerName]
    post_loans   = [ tools.dict_minus(a_loan, "__metadata") for a_loan in loans_ls ]
    loans_df     = pd.DataFrame(post_loans)

    ### or you know: 
    # for loan_dict in loans_ls: 
    #     loan_dict.pop("__metadata")  # No need to save each anyways. 
    # loans_df = pd.DataFrame(loans_ls)

    return loans_df 


def get_person_set():
    the_token    = get_token("header")
    the_hdrs     = get_headers("apis", "headers")

    the_resp     = rq_get(f"{APIS_URL}/v15/bp/PersonSet", 
        auth=tools.BearerAuth(the_token), headers=the_hdrs)

    persons_ls     = the_resp.json()["d"]["results"]  # [metadata : [id, uri, type], borrowerName]
    dict_keys      = ["__metadata", "Roles", "TaxNumbers", "Relation", "Partner", "Correspondence"]
    post_persons   = [ tools.dict_minus(a_loan, dict_keys) for a_loan in persons_ls ]
    persons_df     = (pd.DataFrame(post_persons)
        .assign(ID = lambda df: df.ID.str.pad(10, "left", "0")))

    # for person_dict in persons_ls: 
    #     person_dict.pop("__metadata")  # No need to save each anyways. 
    # persons_df = pd.DataFrame(persons_ls)

    return persons_df


def call_by_id(api_type, type_id=None):
    base_calls = {
        "loans"         : "/v1/lacqan",
        "loan-contract" : "/v1/lacovr", 
        "person-set"    : "/v15/bp"
    }
    loan_apis = ["payment_plan", "balances", "open_items"]
    empl_apis = ["transactions", ]
    
    if   api_type in base_calls: 
        contract_ref = "ContractSet" if type_id is None else f"ContractSet('{type_id}')"
        the_call = f"{base_calls[api_type]}/{contract_ref}"
    elif api_type in loan_apis:
        contract_ref = "ContractSet" if type_id is None else f"ContractSet('{type_id}')"
        camelled = tools.str_snake_to_camel(api_type, first_word_too=True)
        the_call = f"v1/lacovr/{contract_ref}/{camelled}"
    elif api_type in empl_apis:
        account_ref  = "AccountSet"  if type_id is None else f"AccountSet('{type_id}')"
        camelled = tools.str_snake_to_camel(api_type, first_word_too=True)
        the_call = f"v2/bacovr/{account_ref}/{camelled}"
        
    return the_call


# api_type, type_id = "balance", "10000001876-111-MX"
def get_sap_api(api_type, type_id=None):

    # Prepare API call
    api_call = call_by_id(api_type, type_id)
    the_hdrs = get_headers("apis")
    a_token  = get_token(sap_token_file, False, "header")

    the_resp = rq_get(f"{APIS_URL}/{api_call}", 
            auth=tools.BearerAuth(a_token), headers=the_hdrs)
    
    # Prepare API response
    d_resp_ls = the_resp.json()["d"]["results"]

    an_output = d_results(d_resp_ls, api_type)

    return an_output


def get_token(token_file=None, force_new=False, auth_method=None):
    
    if token_file is None: 
        token_file = SITE/"token_file.json"

    if auth_method == "basic":  # Auth as a POST argument. 
        auth_kwargs = {"auth" : HTTPBasicAuth(KEY, SECRET)} 
    else: 
        auth_kwargs = {}        # Auth in Headers. 

    ### Cuando el token existe y es válido. 
    is_valid = False
    if token_file.is_file() and not force_new:
        with open(token_file, "r") as _f:
            prev_token = json.load(_f)
        try:
            # Si no tiene fecha o no es comparable, se le asume expirada.
            # O si no es comparable, se asume el token NO VALIDO.
            is_valid = prev_token["expires_in"] <= dt.now()
            if is_valid: 
                return prev_token
        except (KeyError, TypeError):
            pass
    
    ### Cuando no existe el token, o se requiere uno nuevo.
    if force_new or not is_valid:
        post_kwargs = {
            "headers" : get_headers("token", auth_method),
            "data"    : {
                "grant_type" : "password", 
                "username"   : USER, 
                "password"   : PASS
        }}
        
        post_kwargs.update(auth_kwargs)

        the_resp  = post(AUTH_URL, **post_kwargs)
        the_token = the_resp.json()
        with open(token_file, "w") as _f:
            json.dump(the_token, _f)

    return the_token["access_token"]

#%% Funciones ayudadoras.



def attributes_from_column(attrs_indicator=None):
    sap_attr = pd.read_feather(SITE/"refs/catalogs/sap_attributes.feather")
    possible_columns = list(sap_attr.columns) + ["all"]

    # en_cx, ejemplo, default, postman_default. 
    if attrs_indicator is None: 
        attrs_indicator = "postman_default"
    
    if attrs_indicator not in possible_columns: 
        raise("COLUMN INDICATOR must be one in SAP_ATTR or 'all'.")

    if attrs_indicator == "all":
        attr_df = sap_attr
    else: 
        attr_df = sap_attr.query(f"{attrs_indicator} == 1")
    
    return attr_df["atributo"]


def get_headers(call=None, auth_method=None):
    if call is None:
        call = "apis"
    if auth_method is None:
        auth_method = "basic"

    if call == "apis":
        the_headers = {
                "format"          : "json",
                "Accept-Encoding" : "gzip, deflate",
                "Accept"          : "application/json" }
                # "Authorization"   : f"Bearer {token}"  
                # La autorización se resuelve con la clase de BearerAuth.
    elif call == "token":
        if auth_method   == "basic":
            pass 
        elif auth_method == "header":
            auth_enc    = tools.encode64(f"{KEY}:{SECRET}")
            the_headers = { 
                    "Content-Type"  : "application/x-www-form-urlencoded",
                    "Authorization" :f"Basic {auth_enc}"}

    return the_headers


def d_results(json_item, api_type): 
    api_fields = {
        "loans"         : [ "BorrowerName"], 
        "payment_plan"  : [ "ContractID", "ItemID", "Date", "Category", 
                            "CategoryTxt", "Amount", "Currency", "RemainingDebitAmount"], 
        "balances"      : [ "ID", "Code", "Name", "Amount", "Currency"], 
        "open_items"    : [ ""] }
    
    if api_type == "loans":
        result_0 = json_item["__metadata"]      # Contains:  id, uri, type
        re_match = re.search(r"ContractSet\('(.*)'\)", result_0["uri"])
        result_0["ContractID"] = unquote(re_match.groups()[0])
        
    elif api_type == "payment_plan": 
        pass

    elif api_type == "balances": 
        for res_dict in json_item: 
            res_dict.pop("__metadata")
        result_0 = pd.DataFrame(json_item)

    else:  # "balances", "open_items"
        result_0 = {}        
    
    return result_0


if False:
    loans_df = get_sap_loans("all")
    loans_ids = list(loans_df["ID"])
    
    balances_dfs = {}

    for i, each_id in enumerate(loans_ids): 
        print(i)
        new_df = get_sap_api("balances", each_id)
        balances_dfs[each_id] : new_df.assign(BalanceTS=dt.now().strftime("%Y-%m-%dT%H:%M:%S"))
        loans_ids.remove(each_id)



