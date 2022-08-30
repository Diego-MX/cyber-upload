
from src.utilities import tools

from config import SITE

#%% Preparación
cyber_fields = SITE/"refs/catalogs/Cyber.xlsm.lnk"

dtypes_dict = {'bool': 'bool', 'char': 'string', 'int': 'int64'}
sap_cols_1 = tools.read_excel_table(cyber_fields, 'Output', 'output_cols')
sap_cols = (sap_cols_1[sap_cols_1['leer'].notnull()]
    .assign(**{'leer': lambda df: df['leer'].replace(dtypes_dict)}))
dtypes_cols = {row['columnas']: row['leer'] for _, row in sap_cols.iterrows()}



cyber_types = tools.read_excel_table(cyber_fields, 'Output', 'tipos_datos')


sap_tables  = tools.read_excel_table(cyber_fields, 'Output', 'tablas_sap')
tables_dict = {row['tabla_origen']: row['datalake'] for _, row in sap_tables.iterrows()}

#%% Ejecución
pre_attributes = tools.read_excel_table(cyber_fields, 'Output', 'output')
which_attributes = (pre_attributes['Mandatorio']==1) | pre_attributes['tabla_origen'].notnull()
sap_attributes   = (pre_attributes
    .loc[which_attributes, sap_cols['columnas']]
    .astype(dtypes_cols)
    .assign(**{
        'tabla_origen': lambda df: df['tabla_origen'].replace(tables_dict)})
    .reset_index())

sap_attributes.to_feather("refs/catalogs/cyber_columns.feather")

