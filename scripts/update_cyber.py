
from src.utilities import tools
from config import SITE

#%% Preparación
cyber_fields = SITE/"refs/catalogs/Cyber.xlsm.lnk"

sap_cols_1 = tools.read_excel_table(cyber_fields, 'Output', 'output_cols')
dtypes_dict = {'bool': 'bool', 'char': 'string', 'int': 'int64'}
sap_cols = (sap_cols_1[sap_cols_1['proc'].notnull()]
    .assign(**{'proc': lambda df: df['proc'].replace(dtypes_dict)}))
dtypes_cols = {row['columnas']: row['proc'] for _, row in sap_cols.iterrows()}
weird_bools = [a_col for a_col, a_type in dtypes_cols.items() if a_type == 'bool']

cyber_types = tools.read_excel_table(cyber_fields, 'Output', 'tipos_datos')

sap_tables  = tools.read_excel_table(cyber_fields, 'Output', 'tablas_sap')
tables_dict = {row['tabla_origen']: row['datalake'] for _, row in sap_tables.iterrows()}


#%% Ejecución
pre_attributes = tools.read_excel_table(cyber_fields, 'Output', 'output')

sap_attributes = (pre_attributes[sap_cols['columnas']]
    .apply(lambda a_col: (a_col == 1) if a_col.name in weird_bools else a_col)
    .astype(dtypes_cols)
    .replace({'tabla_origen': tables_dict}))

#%% Checks
use_rows = sap_attributes['tabla_origen'].notnull() | sap_attributes['valor_fijo'].notnull()
non_checks = (sap_attributes
    .assign(**{'check_mandatorio': lambda df: 
            ~df['Mandatorio'] | use_rows})
    .filter(like='check', axis=1)
    .apply(lambda lgl_srs: sum(~lgl_srs), axis=0))

print(non_checks)

#%% And print. 
sub_columns = ['nombre', 'Posición inicial', 'longitud2', 'decimales', 'valor_fijo', 
    'Tipo de dato', 'tabla_origen', 'campo', 'calc']

post_attributes = (sap_attributes
    .loc[use_rows, sub_columns]
    .reset_index())
post_attributes.to_feather("refs/catalogs/cyber_columns.feather")

