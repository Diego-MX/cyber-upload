import sys 
import json 
import base64
import re
from importlib import reload

from openpyxl import load_workbook, utils as xl_utils
import pandas as pd


def reload_from_asterisk(module):
    a_mod = reload(sys.modules[module])
    vars().update(a_mod.__dict__)
    print(f'Module {module} reloaded.')


def str_camel_to_snake(cameled:str):
    # SUBBED, sólo se separa cada dos.  
    subbed = re.sub('(.)([A-Z][a-z]+)',  r'\1_\2', cameled)
    snaked = re.sub('([a-z0-9])([A-Z])', r'\1_\2', subbed).lower()
    return snaked


def str_snake_to_camel(snaked:str, first_word_too=False):
    first, *others = snaked.split('_')
    first_too = first.title() if first_word_too else first.lower()
    cameled = first_too + ''.join(word.title() for word in others)
    return cameled


def snake_2_camel(snake_str):
    first, *others = snake_str.split('_')
    return ''.join([first.lower(), *map(str.title, others)])

        

def select_subdict(a_dict, sub_keys):
    if not sub_keys.is_subset(a_dict.keys()):
        raise ValueError('Trying to select a subdict with keys not contained on large dict.')
    small_dict = dict((k, a_dict[k]) for k in sub_keys)
    return small_dict


def camelize_dict(snake_dict):
    if not isinstance(snake_dict, dict):
        return snake_dict

    def pass_value(a_val): 
        if isinstance(a_val, list):
            passed = list(map(camelize_dict, a_val))
        elif isinstance(a_val, dict):
            passed = camelize_dict(a_val)
        else:
            passed = a_val 
        return passed

    new_dict = dict((k, pass_value(v)) for (k, v) in snake_dict.items())
    return new_dict


def read_excel_table(file, sheet, table): 
    try:
        a_wb = load_workbook(file, data_only=True)
    except xl_utils.exceptions.InvalidFileException: 
        a_wb = load_workbook(shortcut_target(file), data_only=True)
    a_ws  = a_wb[sheet]
    a_tbl = a_ws.tables[table]
    
    rows_ls = [[ cell.value for cell in row ] for row in a_ws[a_tbl.ref]]
    tbl_df  = pd.DataFrame(data=rows_ls[1:], index=None, columns=rows_ls[0])
    return tbl_df


def shortcut_target(filename, file_ext=None):
    def ext_regex(file_ext):
        if file_ext is None: 
            file_ext = 'xlsx'
        if isinstance(file_ext, str):
            ext_reg = file_ext
        elif isinstance(file_ext, list):
            ext_reg = f"{'|'.join(file_ext)}"
        else:
            raise 'FILE_EXT format is not supported.'
        return ext_reg
    
    file_regex = fr'C:\\.*\.{ ext_regex(file_ext) }'
    with open(filename, 'r', encoding='ISO-8859-1') as _f: 
        a_path = a_path = re.findall(file_regex, _f.read(), flags=re.DOTALL)

    if len(a_path) != 1: 
        raise 'Not unique or No shortcut targets found in link.'
    return a_path[0]


def dict_minus(a_dict, key_ls, copy=True): 
    b_dict = a_dict.copy() if copy else a_dict
    for key in key_ls: 
        b_dict.pop(key, None)
    return b_dict


def encode64(a_str): 
    encoded = base64.b64encode(a_str.encode('ascii')).decode('ascii')
    return encoded


def datecols_to_str(a_df):
    b_df = (a_df.select_dtypes(['datetime'])
        .apply(lambda col: col.dt.strftime('%Y-%m-%d'), axis=1))
    return a_df.assign(**b_df)


def dataframe_to_list(a_df, cols_df=None, manual=True): 
    # COLS_DF tiene columnas:
    #  TYPE['datetime', 'date', ...]
    #  ATRIBUTO

    if not manual: 
        dt_df = (a_df.select_dtypes(['datetime'])
            .apply(lambda col: col.dt.strftime('%d-%m-%Y'), axis=1))
        the_list = json.loads(a_df.assign(**dt_df).to_json(orient='records'))

    else: 
        the_types = {'bool': 'logical', 'object': 'character'}

        if cols_df is None:
            cols_df = (a_df.dtypes.replace(the_types)
                .to_frame('dtipo').reset_index()
                .rename(columns={'index': 'nombre'})
                .assign(dtype=lambda df: df.dtype))

        ts_cols     = cols_df.query("dtipo == 'datetime'")['nombre']
        date_cols_0 = cols_df.query("dtipo == 'date'")['nombre']

        str_assign  = {col: str 
                for col in ts_cols if col in a_df.columns.values }

        date_assign = {col: a_df[col].dt.strftime('%Y-%m-%d')
                for col in date_cols_0 if col in a_df.columns.values}
        
        b_df = a_df.astype(str_assign).assign(**date_assign)

        the_list = b_df.to_dict(orient='records')
    return the_list


def set_dataframe_types(a_df, cols_df): 
    # ['char', 'numeric', 'date', 'datetime', 'object']
    dtypes = {'char':str, 'numeric':float, 'object':str, 
        'datetime': 'datetime64[ns]', 'date': 'datetime64[ns]', 'delta': str}

    dtyped = { row.database: dtypes[row.dtipo] for row in cols_df.itertuples()
            if row.database in a_df.columns and row.dtipo in dtypes}
    
    df_typed = a_df.astype(dtyped, errors='ignore')
    
    return df_typed



def curlify(resp_request): 
    the_items = { 
        'method' : resp_request.method, 
        'headers' : ' -H '.join(f"'{k}: {v}'" for (k, v) in resp_request.headers.items()), 
        'data'  : resp_request.body, 
        'uri'   : resp_request.url }
    return "curl - X {method} -H {headers} -d '{data}' '{uri}'".format(**the_items)