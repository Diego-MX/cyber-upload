from base64 import b64encode
import json 
import pandas as pd
from pathlib import WindowsPath
import re
import sys 

#%% Optional packages. 
try: 
    from importlib import reload
except ImportError:
    reload = None

try: 
    from openpyxl import load_workbook
    from openpyxl.utils.exceptions import InvalidFileException
except ImportError: 
    load_workbook = InvalidFileException = None


#%% Define tools functions. 
#  39 DICT_KEYS
#  50 RELOAD_FROM_ASTERISK
#  56 STR_CAMEL_TO_SNAKE
#  63 STR_SNAKE_TO_CAMEL
#  70 SNAKE_2_CAMEL
#  75 SELECT_SUBDICT
#  82 CAMELIZE_DICT
#  99 READ_EXCEL_TABLE
# 112 SHORTCUT_TARGET
# 133 DICT_MINUS
# 140 ENCODE64
# 145 DATECOLS_TO_STR
# 151 DATAFRAME_TO_LIST
# 185 SET_DATAFRAME_TYPES
# 198 CURLIFY


def dict_get2(a_dict, ls_keys, b_val): 
    # Extends a_dict.get(a_key, b_value) to consider several keys. 
    ls_keys_0 = [k for k in ls_keys if k in a_dict]
    a_key = ls_keys_0[0] if ls_keys_0 else None
    a_val = a_dict.get(a_key, b_val)
    return a_val
            

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


def read_excel_table(file, sheet:str, table:str=None, **kwargs): 
    if table is None: 
        table = sheet.lower().replace(' ', '_')

    try:
        a_wb = load_workbook(file, data_only=True)
    except InvalidFileException: 
        a_wb = load_workbook(shortcut_target(file), data_only=True)
    
    a_ws  = a_wb[sheet]

    if table not in a_ws.tables: 
        return None

    a_tbl = a_ws.tables[table]    
    rows_ls = [[ cell.value for cell in row ] for row in a_ws[a_tbl.ref]]
    tbl_df  = pd.DataFrame(data=rows_ls[1:], index=None, columns=rows_ls[0], **kwargs)
    return tbl_df


def shortcut_target(filename, file_ext:str=None):
    
    if file_ext is None: 
        if isinstance(filename, WindowsPath): 
            file_ext = re.findall(r"\.([A-Za-z]{3,4})\.lnk", filename.name)[0]
        else: 
            raise "Couldn't determine file extension."

    file_regex = fr'(C:\\.*\.{file_ext})'

    with open(filename, 'r', encoding='ISO-8859-1') as _f: 
        a_path = re.findall(file_regex, _f.read(), flags=re.DOTALL)

    if len(a_path) != 1: 
        raise 'Not unique or no shortcut targets found in link.'
    return a_path[0]


def dict_minus(a_dict, key_ls, copy=True): 
    b_dict = a_dict.copy() if copy else a_dict
    for key in key_ls: 
        b_dict.pop(key, None)
    return b_dict


encode64 = (lambda a_str: 
    b64encode(a_str.encode('ascii')).decode('ascii'))


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