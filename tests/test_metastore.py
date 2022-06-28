# Databricks notebook source
' '.join(x for x in dbutils.secrets.get("metastore-azure", "sp-lakehylia-id"))
