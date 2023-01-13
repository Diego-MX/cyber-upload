
from itertools import product
import numpy as np
from src.utilities import tools
from config import SITE

#%% Preparación
cyber_fields = SITE/"refs/catalogs/Cyber Specs.xlsm.lnk"

cyber_tables = {
    'sap_saldos': {}, 
    'sap_pagos': {}, 
    'sap_estatus': {}, 
    # 'fiserv_saldos': {}, 
    # 'fiserv_pagos': {}, 
    # 'fiserv_estatus': {}
}

expect_specs = (tools.read_excel_table(cyber_fields, 'general', 'especificacion')
    .set_index('columna'))

data_types = tools.read_excel_table(cyber_fields, 'general', 'tipo_datos')


def excelref_to_feather(xls_df): 
    expect_proc = expect_specs.loc[expect_specs['proc'].notnull(), 'proc']
    weird_bools = expect_proc[expect_proc == 'bool']
    bool_func = (lambda a_col: 
        (a_col == 1) if (a_col.name in weird_bools.index) else a_col)

    df0 = (xls_df[expect_proc.index]
        .apply(bool_func, axis=1)
        .astype(expect_proc)
        .assign(lon_dec = lambda df: df['Longitud'].astype(float)))

    as_lgl = lambda srs: srs.isin([True]).astype(bool)

    specs_2 = df0.assign(
        chk_date   = np.where(as_lgl(df0['Tipo de dato'] == 'DATE'), 
                            as_lgl(df0['lon_dec'] == 8), True), 
        chk_mand   = np.where(as_lgl(df0['Mandatorio']), 
                            as_lgl(df0['columna_valor'] != ''), True), 
        chk_aplica = np.where(as_lgl(df0['Aplica'] == 'N'  ), 
                            as_lgl(df0['columna_valor'] == 'N/A'), True),
        chk_len    = as_lgl(df0['Posición inicial'] + np.floor(df0['lon_dec']) 
                         == df0['Posición inicial'].shift(-1)), 
        chk_name  = ~as_lgl(df0['nombre'].duplicated())
    )

    exec_cols  = expect_specs.loc[expect_specs['ejec'] == 1, 'ejec']
    specs_exec = specs_2[exec_cols.index].reset_index()
        
    checks = (specs_2
        .filter(like='chk', axis=1)
        .apply(lambda srs: sum(~srs), axis=0))

    return (specs_exec, checks)



for a_spec in cyber_tables: 
    print(a_spec)
    spec_ref  = tools.read_excel_table(cyber_fields, a_spec)
    has_join  = tools.read_excel_table(cyber_fields, a_spec, f"{a_spec}_join") 
    (execs, checks) = excelref_to_feather(spec_ref)
    print(checks)

    execs.to_feather(f"refs/catalogs/cyber_{a_spec}.feather")
    if has_join is not None: 
        has_join.to_feather(f"refs/catalogs/cyber_{a_spec}_joins.feather")
    

