from datetime import datetime as dt 
import numpy as np
from pytz import timezone

from src.utilities import tools
from src.platform_resources import AzureResourcer
from config import (ConfigEnviron, ENV, SERVER, SITE)

#%% Preparación

cyber_fields = SITE/"refs/catalogs/Cyber Specs.xlsm.lnk"

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
        chk_name   =~as_lgl(df0['nombre'].duplicated()))

    exec_cols  = expect_specs.loc[expect_specs['ejec'] == 1, 'ejec']
    specs_exec = specs_2[exec_cols.index].reset_index()
        
    checks = (specs_2
        .filter(like='chk', axis=1)
        .apply(lambda srs: sum(~srs), axis=0))

    return (specs_exec, checks)


if __name__ == '__main__': 

    app_environ = ConfigEnviron(ENV, SERVER)
    az_manager = AzureResourcer(app_environ)
    print(f"Variables: (Env, Server)={ENV, SERVER}")

    blobs_paths = "cx/collections/cyber/spec_files"
    
    cyber_tasks = [
        'sap_saldos' , # 'fiserv_saldos' ,
        'sap_pagos'  , # 'fiserv_pagos'  ,
        'sap_estatus'] # 'fiserv_estatus'

    for each_task in cyber_tasks: 
        print(f"\nRunning task: {each_task}")
        spec_local = f"refs/catalogs/cyber_{each_task}.feather"
        join_local = f"refs/catalogs/cyber_{each_task}_joins.csv"

        now_str = dt.now(tz=timezone('America/Mexico_City')).strftime('%Y-%m-%d_%H:%M')

        blob_1  = f"{blobs_paths}/{each_task}_specs_latest.feather" 
        blob_2  = f"{blobs_paths}/{each_task}_specs_{now_str}.feather" 
        blob_3  = f"{blobs_paths}/{each_task}_joins_latest.csv" 
        blob_4  = f"{blobs_paths}/{each_task}_joins_{now_str}.csv" 
    
        spec_ref = tools.read_excel_table(cyber_fields, each_task)
        has_join = tools.read_excel_table(cyber_fields, each_task, f"{each_task}_join") 
        (execs, checks) = excelref_to_feather(spec_ref)
        execs.to_feather(spec_local)
        
        az_manager.upload_storage_blob(spec_local, blob_1, 'gold', overwrite=True, verbose=1)
        az_manager.upload_storage_blob(spec_local, blob_2, 'gold')
        print(checks)

        if has_join is not None: 
            has_join.to_csv(join_local, index=False)
            az_manager.upload_storage_blob(join_local, blob_3, 'gold', overwrite=True)
            az_manager.upload_storage_blob(join_local, blob_4, 'gold')





