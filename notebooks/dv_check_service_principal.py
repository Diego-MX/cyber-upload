# Databricks notebook source
a_client = dbutils.secrets.get('eh-core-banking', 'sp-core-events-secret')
' '.join(x for x in a_client)
