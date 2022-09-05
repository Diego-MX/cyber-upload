
from src.utilities import tools
from config import SITE

#%% Preparación
cyber_fields = SITE/"refs/catalogs/Cyber.xlsm.lnk"

# todas las columnas
cyber_meta = (tools.read_excel_table(cyber_fields, 'Output', 'output_cols')
    .query(f"proc.notnull()"))


replace_types = {row['columnas']: row['proc'] 
        for _, row in cyber_meta.iterrows()}

weird_bools = filter(lambda k: replace_types[k] == 'bool', replace_types)


tables_meta = tools.read_excel_table(cyber_fields, 'Output', 'tablas_sap')
replace_tbls = {row['tabla_origen']: row['datalake'] 
        for _, row in tables_meta.iterrows()}


#%% Ejecución
cyber_refs = tools.read_excel_table(cyber_fields, 'Output', 'output')

cyber_ref = (cyber_refs[cyber_meta['columnas']]
    .apply(lambda a_col: (a_col == 1) if a_col.name in weird_bools else a_col)
    .astype(replace_types)
    .replace({'tabla_origen': replace_tbls}))

#%% Checks

non_checks = (cyber_ref
    .filter(like='check', axis=1)
    .apply(lambda lgl_srs: sum(~lgl_srs), axis=0))

print(non_checks)

#%% And print. 
sub_columns = cyber_meta['columnas'][cyber_meta['ejec']==1]

post_attributes = (cyber_ref[sub_columns]
    .reset_index())

post_attributes.to_feather("refs/catalogs/cyber_columns.feather")

