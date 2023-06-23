from datetime import date
from delta.tables import DeltaTable as Δ
from functools import reduce
from pyspark.sql import (functions as F, DataFrame as spk_DF, 
    SparkSession as spk_Sn, GroupedData, Column)
import re
from typing import Union

from .tools import MatchCase



class EpicDF(spk_DF): 
    '''
    Add functionality to Spark DataFrame. 
    ''' 

    main_methods = ['select', 'join', 'filter', 'fillna', 'withColumn', 
        'withColumnRenamed', 'dropDuplicates']


    def return_epic(self, ff): 
        epic_ff = lambda *a, **kw: EpicDF(ff(*a, **kw))
        return epic_ff
    
    def epic_method(self, ff_name): 
        base_ff = getattr(self, ff_name)
        epic_ff = self.return_epic(base_ff)
        setattr(self, ff_name, epic_ff)
        return 


    def __init__(self, arg1=None, arg2=None): 
        if isinstance(arg1, spk_DF): 
            df = arg1
        elif isinstance(arg1, spk_Sn) and Δ.isDeltaTable(arg1, arg2): 
            df = arg1.read.load(arg2)
        else: 
            raise TypeError(f"EpicDF init with types (SparkDF) or (spark, path)")       
        
        super().__init__(df._jdf, df.sql_ctx)
        self._df = df

        for b_method in self.main_methods: 
            self.epic_method(b_method)
        # Which is equivalent to: 
        # @return_epic
        # def method(self, *args, **kwargs): 
        #    return self.method(*args, **kwargs)


    def select_plus(self, cols=Union[dict, list]): 
        if isinstance(cols, dict): 
            cols = [as_column(kk).alias(vv) for kk, vv in cols.items()]
        return self.select(*cols)
    
    def filter_plus(self, *filters): 
        λ_filter = lambda x_df, fltr: x_df.filter(fltr)
        return reduce(λ_filter, filters, self)
        
    def with_column_plus(self, mutate: dict): 
        λ_with = lambda x_df, kk_vv: x_df.withColumn(*kk_vv)
        other  = reduce(λ_with, mutate.items(), self)  
        return other

    def with_column_renamed_plus(self, rename: dict): 
        λ_rename = lambda x_df, kk_vv: x_df.withColumnRenamed(*kk_vv)
        other    = reduce(λ_rename, rename.items(), self)
        return other


    def fill_na_plus(self, na_plus: dict): 
        λ_default = lambda x_df, kk_vv: x_df.fillna(kk_vv[0], kk_vv[1]) 
        λ_date    = lambda x_df, kk_vv: x_df.with_column_plus({
                cc: F.coalesce(cc, F.lit(kk_vv[0])) for cc in kk_vv[1]})
        
        # λ_via  = lambda x_df, kk_vv: type(kk_vv[0])
        # λ_fill = MatchCase(via=λ_via, 
        #     case_dict={
        #         None: λ_default, 
        #         date: λ_date })
        def λ_fill(x_df, kk_vv): 
            kk, vv = kk_vv
            if isinstance(kk, date): 
                return λ_date(x_df, kk_vv)
            else: 
                return λ_default(x_df, kk_vv)

        return reduce(λ_fill, na_plus.items(), self)


    def join_plus(self, *args, **kwargs):
        if 'suffix' in kwargs: 
            suffix = kwargs['suffix']
            other  = kwargs['other']
            how    = kwargs['how']
            on     = kwargs['on']

            rename_cols = [col for col in self.columns
                if col in other.columns and col not in on]

            other_plus = other.with_renamed({col: f'{col}_{suffix[1]}' 
                    for col in rename_cols})
            
            joined = (self
                .with_renamed({col: f'{col}_{suffix[0]}' 
                    for col in rename_cols})
                .join(other=other_plus, on=on, how=how))
        else: 
            joined = self.join(*args, **kwargs)
        return joined


    def groupBy(self, *args, **kwargs): 
        return EpicGroupDF(super().groupBy(*args, **kwargs), self._df)


    def __repr__(self):
        return super().__repr__().replace('DataFrame', 'EpicDF')
        

    # These are still not Mature.     
    def as_fixed_width(self, fixer:dict): 
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


    def save_as_file(self, path_dir:str, path_file, **kwargs):
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(self.sql_ctx.sparkSession)
        
        # paths in abfss://container... mode. 
        std_args = {
            'mode'  : 'overwrite', 
            'header': 'false'}
        std_args.update(kwargs)
        
        (self._df.coalesce(1).write
            .mode(std_args['mode'])
            .option('header', std_args['header'])
            .text(path_dir))
        
        f_extras = [f_info for f_info in dbutils.fs.ls(path_dir) 
            if f_info.name.startswith('part-')]
        
        if len(f_extras) != 1: 
            raise "Expect only one file starting with 'PART'"

        dbutils.fs.mv(f_extras[0].path, path_file)
        return

    def with_columns(self, mutate:dict):
        print("Use WITH_COLUMN_PLUS")
        λ_with = lambda x_df, kk_vv: x_df.withColumn(*kk_vv)
        other  = reduce(λ_with, mutate.items(), self)  
        return other

    def with_renamed(self, rename: dict): 
        print("Use WITH_COLUMN_RENAMED_PLUS")
        λ_rename = lambda x_df, kk_vv: x_df.withColumnRenamed(*kk_vv)
        other    = reduce(λ_rename, rename.items(), self)
        return other



class EpicGroupDF(GroupedData): 
    def __init__(self, gdf, df): 
        super().__init__(gdf._jgd, df)

    def __repr__(self): 
        super().__repr__().replace('GroupedData', 'EpicGroupDF')

    def agg(self, *args, **kwargs): 
        return EpicDF(super().agg(*args, **kwargs))


def as_column(a_col: Union[Column, str]) -> str: 
    if   isinstance(a_col, Column): 
        return a_col
    elif isinstance(a_col, str): 
        return F.col(a_col)
    else: 
        print(f"Check type: {type(a_col)}")
        return a_col


def column_name(a_col: Union[Column, str]) -> str: 
    if isinstance(a_col, str): 
        return a_col
     
    reg_alias = r"Column\<'(.*) AS (.*)'\>"
    non_alias = r"Column\<'(.*)'\>"
    
    a_match   = re.match(reg_alias, str(a_col))
    n_match   = re.match(non_alias, str(a_col))
    
    the_call = (a_match.group(2) if a_match is not None 
           else n_match.group(1)) 
    return the_call


def when_plus(when_dict, key='value') -> Column: 
    ''' 
    Depends on module F ~ pyspark.sql.functions
    KEY ='value'     -> ...when(vv, kk)
    KEY ='condition' -> ...when(F.col(kk), vv)
    {None: vv}       -> ...otherwise(vv)
    '''
    if   key == 'value': 
        λ_when = lambda ff, k_v: ff.when(k_v[1], k_v[0])
    elif key == 'condition': 
        λ_when = lambda ff, k_v: ff.when(k_v[0], k_v[1])

    w_dict = when_dict.copy()
    w_else = w_dict.pop(None, F.lit(None))
    F_when = reduce(λ_when, when_dict.items(), F)
    return F_when.otherwise(F.lit(w_else))



    
