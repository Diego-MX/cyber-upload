
from src.utilities import tools
from config import SITE

#%% Preparación
cyber_fields = SITE/"refs/catalogs/Cyber.xlsm.lnk"

dtypes_dict = {'bool': 'bool', 'char': 'string', 'int': 'int64'}
sap_cols_1 = tools.read_excel_table(cyber_fields, 'Output', 'output_cols')
sap_cols = (sap_cols_1[sap_cols_1['proc'].notnull()]
    .assign(**{'proc': lambda df: df['proc'].replace(dtypes_dict)}))
dtypes_cols = {row['columnas']: row['proc'] for _, row in sap_cols.iterrows()}

weird_bools = { a_col: (lambda df: df[a_col] == 1)
    for a_col, a_type in dtypes_cols.items() if a_type == 'bool'}

cyber_types = tools.read_excel_table(cyber_fields, 'Output', 'tipos_datos')


sap_tables  = tools.read_excel_table(cyber_fields, 'Output', 'tablas_sap')
tables_dict = {row['tabla_origen']: row['datalake'] for _, row in sap_tables.iterrows()}


#%% Ejecución
pre_attributes = tools.read_excel_table(cyber_fields, 'Output', 'output')

weird_bools = { a_col: lambda df: df[a_col] == 1
    for a_col, a_type in dtypes_cols.items() if a_type == 'bool'}

weirder_bools2 = {a_col: (lambda df: df[a_col] == 1) for a_col in ['Mandatorio', ]}  #, 'tiene_calculo', 'check_NA', 'check_size'
# Por alguna razón WEIRD_BOOLS no funciona. 

sap_attributes = (pre_attributes[sap_cols['columnas']]
    .assign(**weirder_bools2 )
    .astype(dtypes_cols)
    .replace({'tabla_origen': tables_dict}))

#%% Checks
non_checks = (sap_attributes
    .assign(**{'check_mandatorio': lambda df: 
            ~df['Mandatorio'] | df['tabla_origen'].notnull()})
    .filter(like='check', axis=1)
    .apply(lambda lgl_srs: sum(~lgl_srs), axis=0))

print(non_checks)

#%% And print. 
sub_columns = ['nombre', 'Posición inicial', 'longitud2', 'decimales', 'valor_fijo', 'Tipo de dato', 'tabla_origen', ]
post_attributes = (sap_attributes
    .loc[sap_attributes['tabla_origen'].notnull(), sub_columns]
    .reset_index())
post_attributes.to_feather("refs/catalogs/cyber_columns.feather")

