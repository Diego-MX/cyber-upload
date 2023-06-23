from datetime import date
from delta.tables import DeltaTable as Δ 
from functools import reduce
import pandas as pd
from pandas import DataFrame as pd_DF
from pyspark.sql import (functions as F, types as T, Column, 
    DataFrame as spk_DF, sparkSession as spk_Sn)
from typing import Union


class EpicDF(spk_DF): 
    '''
    Add functionality to Spark DataFrame. 
    '''

    def __init__(self, arg1=None, arg2=None): 
        if isinstance(arg1, spk_DF): 
            df = arg1
        elif isinstance(arg1, spk_Sn) and Δ.isDeltaTable(arg2): 
            df = arg1.read.load(arg2)
        else: 
            raise TypeError(f"EpicDF init with types (SparkDF) or (spark, ) ")       
        
        super().__init__(df._jdf, df.sql_ctx)
        self._df = df

    def return_epic(ff): 
        epic_ff = lambda *a, **kw: EpicDF(ff(*a, **kw)) 
        return epic_ff

    @return_epic
    def select(self, *args, **kwargs): 
        return self.select(*args, **kwargs)

    @return_epic
    def filter(self, *args, **kwargs): 
        return self.filter(*args, **kwargs)

    
    # Estos no necesitan el wrapper, porque los individuales lo tienen. 
    def with_columns(self, mutate: dict): 
        λ_with = lambda x_df, kk_vv: x_df.withColumn(*kk_vv)
        other  = reduce(λ_with, mutate.items(), self)  
        return other

    @return_epic
    def with_renamed(self, rename: dict): 
        λ_rename = lambda x_df, kk_vv: x_df.withColumnRenamed(*kk_vv)
        other    = reduce(λ_rename, rename.items(), self)
        return other

    @return_epic
    def fill_na_plus(self, na_plus: dict): 
        def λ_fill(x_df, kk_vv): 
            kk, vv = kk_vv
            if not isinstance(kk, date): 
                y_df = x_df.fillna(*kk_vv)
            elif isinstance(kk, date): 
                λ_dt = {cc: F.when(F.col(cc).isNull(), kk).otherwise(cc)
                    for cc in vv} 
                y_df = x_df.with_columns(λ_dt)
            return y_df

        other = reduce(λ_fill, na_plus.items(), self)
        return other

    def __repr__(self):
        return f"EpicDF({super().__repr__()})"
        
        

    @return_epic
    def as_fixed_width(self, fixer:dict) -> spk_DF: 
        '''
        Fixer is a dictionary that gives instructions to fixed width. 
        Notice that Date values are cast before, but Numeric/String after
        replacing NAs. 
        '''
        fix_date = {k:v for k,v in fixer['0-fill'].items() if isinstance(k, date)}
        fix_gral = {k:v for k,v in fixer['0-fill'].items() if not isinstance(k, date)}
        
        other = (self
            .fill_na_plus(fix_gral)
            .select(fixer['1-cast'])
            .fill_na_plus(fix_date)
            .select(fixer['2-string']))
        return other




class CyberData(): 
    'General Namespace to handle Cyber Specifications.'

    def __init__(self, spark): 
        self.spark = spark

    na_types = {
        'str' : '', 
        'dbl' : 0, 
        'int' : 0, 
        'date': date(1900, 1, 1)}

    spk_types = {
        'str' : T.StringType, 
        'dbl' : T.DoubleType, 
        'int' : T.IntegerType, 
        'date': T.DateType}

    txn_codes = ["092703", "092800", "500027", "550021", "550022", "550023", 
        "550024", "550403", "550908", "650404", "650710", "650712", "650713", 
        "650716", "650717", "650718", "650719", "650720", "750001", "750025", 
        "850003", "850004", "850005", "850006", "850007", "958800"]



    def specs_setup(self, a_file:str) -> pd_DF:
        c_formats = {
            'int' : '%0{}d', 
            'dbl' : '%0{}.{}f', 
            'dec' : '%0{}.{}d',  # Puede ser '%0{}.{}f'
            'str' : '%-{}.{}s', 
            'date': '%8.8d', 
            'long': '%0{}d'}
        
        specs_0 = pd.read_feather(a_file).set_index('nombre')

        specs_df = specs_0.assign(
            width       = specs_0['Longitud'].astype(float).astype(int), 
            width_1     = lambda df: df['width'].where(df['PyType'] != 'dbl', df['width'] + 1),
            precision_1 = specs_0['Longitud'].str.split('.').str[1], 
            precision   = lambda df: np.where(df['PyType'] == 'dbl', 
                                    df['precision_1'], df['width']), 
            is_na = ( specs_0['columna_valor'].isnull() 
                    | specs_0['columna_valor'].isna() 
                    |(specs_0['columna_valor'] == 'N/A').isin([True])),
            x_format = lambda df: [
                c_formats[rr['PyType']].format(rr['width_1'], rr['precision']) 
                for _, rr in df.iterrows()], 
            y_format = lambda df: df['x_format'].str.replace(r'\.\d*', '', regex=True),
            c_format = lambda df: df['y_format'].where(df['PyType'] == 'int', df['x_format']), 
            s_format = lambda df: ["%{}.{}s".format(wth, wth) for wth in df['width']])

        return specs_df


    def fxw_converter(self, specs_df:pd_DF) -> dict:
        '''
        Input : SPECS_DF: [name; PyType, c_format, s_format]
        Output: [FILL_NA, CAST_TYPES, DATE_COLS, FXW] 
        ### Dates are cast before NAs, but the rest after. 
        '''
        str_λs = {
            'str' : lambda nn, cc: F.format_string(cc, F.col(nn)), 
            'dbl' : lambda nn, cc: F.regexp_replace(F.format_string(cc, F.col(nn)), '[\.,]', ''), 
            'int' : lambda nn, cc: F.format_string(str(cc), nn), 
            'date': lambda nn, cc: F.when(F.col(nn) == self.na_types['date'], F.lit('00000000')
                    ).otherwise(F.date_format(nn, 'MMddyyyy')) }

        def row_formatter(name, a_row): 
            py_type = a_row['PyType']
            fmt_1 = str_λs[py_type](name, a_row['c_format'])
            fmt_2 = F.format_string(a_row['s_format'], fmt_1)
            return fmt_2

        fill_0 = { 
            0 : specs_df.index[specs_df['PyType'].isin('int', 'dbl')].tolist(), 
            '': specs_df.index[specs_df['PyType'] == 'str' ].tolist() }
            # date(1900, 1, 1): specs_df.index[specs_df['PyType'] == 'date'].tolist()
        
        cast_1 = [F.col(name).cast(self.spk_types[py_type]()) 
            for name, py_type in specs_df['PyType']]
        
        strg_2 = [row_formatter(nn, rr).alias(nn) 
            for nn, rr in specs_df.iterrows()]

        return {'0-fill': fill_0, '1-cast': cast_1, '2-string': strg_2}




def upsert_into_delta(spark, new_tbl:spk_DF, base_path, on_cols): 
    on_str = " AND ".join(f"t1.{cc} = t0.{cc}" for cc in on_cols)
    
    (Δ.forPath(spark, base_path).alias('t0')
        .merge(new_tbl.alias('t1'), on_str)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    return 


def upsert_by_group(spark, new_tbl:spk_DF, base_path, on_cols):
    # COLS: [0]  -> número de filas. 
    #       [1]  -> ranking de fila. 
    #       [2:] -> join_on. 
    c0, c1, *c2 = on_cols
    by_groups = (new_tbl.groupBy(c2).count())
    merger = (spark.read
        .load(base_path)
        .select(c1, *c2)
        .join(by_groups, how='inner', on=c2)
        .join(new_tbl, how='left', on=[c1, *c2])
        .withColumn(c0, F.coalesce(c0, 'count')))
    
    cond_eq = ' AND '.join(f"(t1.{cc} = t0.{cc})" for cc in [c1, *c2]) 
    cond_gr = ' AND '.join(f"(t1.{c0} < t0.{c1})", 
                         *(f"(t1.{cc} = t0.{cc})" for cc in c2))
    (Δ.forPath(spark, base_path).alias('Δ0')
        .merge(merger.alias('Δ1'), cond_eq)
        .whenMatchedDelete(cond_gr)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    return


def pd_print(a_df: pd.DataFrame, **kwargs):
    the_options = {
        'max_rows'   : None, 
        'max_columns': None, 
        'width'      : 180}
    the_options.update(kwargs)
    optns_ls = sum(([f"display.{kk}", vv] 
        for kk, vv in the_options.items()), start=[])
    with pd.option_context(*optns_ls):
        print(a_df)
    return






def save_as_file(a_df: spk_DF, path_dir:str, path_file, **kwargs):
    # paths in abfss://container... mode. 
    std_args = {
        'mode'  : 'overwrite', 
        'header': 'false'}
    std_args.update(kwargs)
    
    (a_df.coalesce(1).write
         .mode(std_args['mode'])
         .option('header', std_args['header'])
         .text(path_dir))
    
    f_extras = [f_info for f_info in dbutils.fs.ls(path_dir) 
            if f_info.name.startswith('part-')]
    
    if len(f_extras) != 1: 
        raise "Expect only one file starting with 'PART'"
    
    dbutils.fs.mv(f_extras[0].path, path_file)
    return



def column_name(a_col: Union[Column, str]) -> str: 
    if isinstance(a_col, str): 
        return a_col
        
    reg_alias = r"Column\<'(.*) AS (.*)'\>"
    non_alias = r"Column\<'(.*)'\>"
    a_match = re.match(reg_alias, str(a_col))
    n_match = re.match(non_alias, str(a_col))
    
    the_call = (a_match.group(2) if a_match is not None 
           else n_match.group(1)) 
    return the_call



       
# @F.pandas_udf(T.StringType())
# def udf_encode(x_str:pd_S, encoding:str='ascii', elementwise:bool=True)->str:   
#     # Se consideró también ISO-8859-1 que es lo más parecido a ANSI/ASCII
#     if elementwise: 
#         λ_enc = lambda xx: xx.encode(encoding, 'ignore')
#         y_str = x_str.str.normalize('NFKD').map(λ_enc) 
#     else: 
#         y_str = (normalize('NFKD', x_str)
#             .encode(encoding, 'ignore')
#             .decode(encoding))
#     return y_str
