# Databricks notebook source
from epic_py.identity import EpicIdentity

setup = {
    'service-principal' : {
        'client_id'       : 'sp-core-events-client', 
        'client_secret'   : 'sp-core-events-secret', 
        'tenant_id'       : 'aad-tenant-id', 
        'subscription_id' : 'sp-core-events-suscription' } , 
    'databricks-scope': 'eh-core-banking'}


