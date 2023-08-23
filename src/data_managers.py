
from collections import OrderedDict, defaultdict
from datetime import datetime as dt, date
import numpy as np
import pandas as pd
from pandas import DataFrame as pd_DF
from pyspark.sql import (functions as F, types as T, Window as W)
from pytz import timezone
from toolz import compose, curry, pipe, valmap
import re

from epic_py.delta import (EpicDF, TypeHandler, 
    column_name, when_plus)
from epic_py.tools import MatchCase, partial2, method_getter

item_namer = lambda names: compose(dict, partial2(zip, names))
## (kk, vv) -> {nm[0]: kk, nm[1]: vv}


class CyberData(): 
    '''General Namespace to handle Cyber Specifications.'''

    def __init__(self, spark): 
        self.spark = spark
        self.set_defaults()
  
    def prepare_source(self, which, path, **kwargs): 
        base_df = EpicDF(self.spark, path)
        if which == 'balances': 
            bal_cols = {
                'x3_96': F.col('code_3') + F.col('code_96')}

            x_df = (base_df   # type: ignore
                .dropDuplicates()
                .with_column_plus(bal_cols))

        elif which == 'loan-contracts':
            
            open_items = (kwargs['open_items']
                .select('ID', 'oldest_date'))

            where_loans = [F.col('LifeCycleStatusTxt') == 'activos, utilizados']

            status_dict = OrderedDict({
                ('VIGENTE',  '303') : (F.col('LifeCycleStatus').isin(['20', '30'])) 
                                    & (F.col('OverdueDays') == 0), 
                ('VENCIDO',  '000') : (F.col('LifeCycleStatus').isin(['20', '30'])) 
                                    & (F.col('OverdueDays') >  0),
                ('LIQUIDADO','302') :  F.col('LifeCycleStatus') == '50', 
                ('undefined','---') :  None})
            
            usgaap = [
                ((F.col('StageLevel') < 3) & (F.col('OverdueDays') == 0), F.lit(None)), 
                ((F.col('StageLevel') < 3) & (F.col('OverdueDays') >  0), F.col('oldest_date')+90), 
                ((F.col('StageLevel')== 3),   F.col('EvaluationDate')), 
                (None, F.lit(None))]
            
            loan_cols = OrderedDict({
                'ContractID'   : F.col('ID'), 
                'person_id'    : F.col('BorrowerID'),
                'borrower_mod' : F.concat(F.lit('B0'), F.col('BorrowerID')),
                'interes_amort': F.col('NextPaymentInterestAmt')
                               + F.col('NextExemptInterestAmount'), 
                'yesterday'    : F.date_add(F.current_date(), -1), 
                'usgaap_date'  : when_plus(usgaap),
                'status_2' : when_plus([(vv, kk[0]) 
                            for kk, vv in status_dict.items()]), 
                'status_3' : when_plus([(vv, kk[1]) 
                            for kk, vv in status_dict.items()])})

            repay_dict = {'MENSUAL': 'MT', 'SEMANAL': 'WK', 'QUINCENAL': 'FT'}
            repay_rows = map(item_namer(['repay_freq', 'RepaymentFrequency']), repay_dict)
            repay_df   = self.spark.createDataFrame(repay_rows)

            x_df = (base_df   # type: ignore
                .filter_plus(*where_loans) 
                .join(open_items, on='ID', how='left')
                .join(repay_df, on='RepaymentFrequency', how='left')
                .with_column_plus(loan_cols))
            
        elif which == 'open-items-wide': 
            id_cols   = ['ContractID', 'epic_date', 'ID']
            rec_types = ['511010', '511100', '990004', '991100', '511200', '990006'] 

            w_duedate = W.partitionBy(*id_cols).orderBy('DueDate')
            min_date = lambda cond: (F.lit(1) == F.when(cond, 
                    F.row_number().over(w_duedate)).otherwise(-1))

            open_cols = OrderedDict({
                'yesterday'  : F.current_date() - 1, 
                'ID'         : F.col('ContractID'), 
                'interest'   : F.col('991100') + F.col('511100'), 
                'iva'        : F.col('990004'), 
                'is_due'     : F.col('DueDate') >= F.current_date(),  
                'is_default' : F.col('StatusCategory').isin(['2','3']),
                'is_current' : min_date(F.col('is_due')), 
                'is_oldest'  : min_date(F.col('is_default'))})
                
            group_cols = {
                'current' : {
                    'current_amount'   : F.col('amount')}, 
                'oldest': OrderedDict({
                    'oldest_date'      : F.col('DueDate'), 
                    'oldest_uncleared' : F.col('uncleared'), 
                    'oldest_eval_date' : F.col('DueDate') + 90, 
                    'overdue_days'     : pipe(F.col('DueDate'), 
                        partial2(F.datediff, 'yesterday'), 
                        partial2(F.coalesce, ..., F.lit(0)))})} 
            
            y_df = (base_df   # type: ignore
                .fillna(0, subset=rec_types)
                .with_column_plus(open_cols))
            
            x1_df = (y_df
                .filter('is_current')
                .with_column_plus(group_cols['current']))
            
            x2_df = (y_df
                .filter(F.col('is_oldest'))
                .with_column_plus(group_cols['oldest']))

            x_df = (x1_df
                .join(x2_df, on=id_cols, how='outer')
                .select(*id_cols, 
                    *group_cols['current'].keys(), 
                    *group_cols['oldest' ].keys()))

        elif which == 'open-items-long': 
            id_cols   = ['ContractID', 'epic_date', 'ID']
            w_duedate = W.partitionBy(*id_cols).orderBy('DueDate')
            single_cols = OrderedDict({
                'ID'        : F.col('ContractID'),
                'cleared'   : pipe(F.col('ReceivableDescription'), 
                        partial2(F.regexp_extract, ..., r"Cleared: ([\d\.]+)", 1), 
                        method_getter('cast', T.DecimalType(20, 2)), 
                        partial2(F.coalesce, ..., F.lit(0))), 
                'uncleared'  : F.col('Amount') - F.col('cleared'), 
                '_max_date'  : F.max('DueDate').over(w_duedate),
                'yesterday'  : F.current_date() - 1, 
                'is_default' : F.col('StatusCategory').isin(['2','3']),
                'min_default': F.when(F.col('is_default'), F.col('DueDate'))
                    .otherwise(F.col('_max_date'))
            })
            
            uncleared_if = (lambda cond: 
                F.sum(F.when(cond, F.col('uncleared')).otherwise(0)))

            group_cols = {
                'default_uncleared' : uncleared_if(F.col('is_default')), 
                'default_interest'  : uncleared_if(F.col('ReceivableType').isin(['991100', '511100'])), 
                'default_iva'       : uncleared_if(F.col('ReceivableType') == '990004')}
                
            x_df = (base_df
                .with_column_plus(single_cols)
                .filter(F.col('is_default'))
                .groupBy(*id_cols)
                .agg_plus(group_cols))
            
        elif which == 'person-set':
            states_str = [
                "AGU,1",  "BCN,2",  "BCS,3",  "CAM,4",  "COA,5",  "COL,6",  "CHH,8",  "CHP,7", 
                "CMX,9",  "DUR,10", "GUA,11", "GRO,12", "HID,13", "JAL,14", "MEX,15", "MIC,16", 
                "MOR,17", "NAY,18", "NLE,19", "OAX,20", "PUE,21", "QUE,22", "ROO,23", "SLP,24", 
                "SIN,25", "SON,26", "TAB,27", "TAM,28", "TLA,29", "VER,30", "YUC,31", "ZAC,32"]

            states_data = [ item_namer(['AddressRegion', 'state_key'])(split)
                for split in map(method_getter('split', ','), states_str)]

            states_df = self.spark.createDataFrame(states_data)
            
            persons_cols = {   
                'LastNameP' : F.col('LastName'),
                'LastNameM' : F.col('LastName2'), 
                'full_name' : F.concat_ws(' ', 'FirstName', 'MiddleName', 'LastName', 'LastName2'), 
                'address2'  : F.concat_ws(' ', 'AddressStreet', 'AddressHouseID'), 
                'yesterday' : F.date_add(F.current_date(), -1)}

            x_df = (base_df     # type: ignore
                .filter(F.col('ID').isNotNull())
                .join(states_df, how='left', on='AddressRegion')
                .with_column_plus(persons_cols))

        elif which == 'txns-grp': 
            pymt_codes = [550021, 550022, 550023, 550024, 550403, 550908]

            by_older = W.partitionBy('AccountID', 'is_payment').orderBy(F.col('ValueDate'))
            by_newer = W.partitionBy('AccountID', 'is_payment').orderBy(F.col('ValueDate').desc())
            
            txn_cols = OrderedDict({
                'is_payment': F.col('TransactionTypeCode').isin(pymt_codes),
                'is_oldest' : F.row_number().over(by_older) == 1,
                'is_newest' : F.row_number().over(by_newer) == 1})

            map_cols = {
                'oldest': {
                    'AccountID': 'account_id', 
                    'ValueDate': 'first_date'}, 
                'newest': {
                    'AccountID': 'account_id', 
                    'ValueDate': 'last_date',
                    'Amount'   : 'last_amount', 
                    'AmountAc' : 'last_amount_local'} }

            y_df = (base_df
                .with_column_plus(txn_cols)
                .filter(F.col('is_payment')))

            x1_df = (y_df      # type: ignore
                .filter(F.col('is_newest'))
                .with_column_renamed_plus(map_cols['newest']))
            
            x2_df = (y_df      # type: ignore
                .filter(F.col('is_oldest'))
                .with_column_renamed_plus(map_cols['oldest']))  

            x_df = (x1_df
                .join(x2_df, on='account_id', how='inner')
                .withColumnRenamed('account_id', 'AccountID'))

        elif which == 'txns-set': 
            txn_codes = [92703,  92800, 500027, 550021, 550022, 550023, 
                550024, 550403, 550908, 650404, 650710, 650712, 650713, 
                650716, 650717, 650718, 650719, 650720, 750001, 750025, 
                850003, 850004, 850005, 850006, 850007, 958800]
            
            with_rename = {
                'yesterday'         : F.current_date() - 1, 
                'TransactionTypeTxt': F.col('TransactionTypeName') , 
                'TransactionType'   : F.col('TransactionTypeCode'),  
                'ContractID'        : F.col('AccountID')}
                
            x_df = (base_df
                .with_column_plus(with_rename)
                .filter_plus(
                    F.col('TransactionTypeCode').isin(txn_codes), 
                    F.col('ValueDate') == F.col('yesterday')))

        else:  
            raise Exception(f"WHICH (source) is not specified")
        
        return x_df


    def fxw_converter(self, specs_df:pd_DF): 
        '''
        Input : SPECS_DF: [name; PyType, c_format, s_format]
        Output: [FILL_NA, CAST_TYPES, DATE_COLS, FXW] 
        ### Dates are cast before NAs, but the rest after. 
        '''
        
        @F.pandas_udf('string')     # type: ignore
        def latinize(srs: pd.Series) -> pd.Series:
            lat_srs = (srs.str
                .normalize('NFKD').str
                .encode('ascii', errors='ignore').str
                .decode('utf-8'))
            return lat_srs 

        str_λs = {
            'str' : lambda rr: F.format_string(rr['c_format'], rr['nombre']), 
            'dbl' : lambda rr: F.regexp_replace(F.format_string(rr['c_format'], rr['nombre']), '[\.,]', ''), 
            'int' : lambda rr: F.format_string(str(rr['c_format']), rr['nombre']), 
            'date': lambda rr: F.when(F.col(rr['nombre']) == self.na_types['date'], F.lit('00000000')
                    ).otherwise(F.date_format(rr['nombre'], 'MMddyyyy')) }

        def row_formatter(a_row): 
            py_type = a_row['PyType']
            fmt_1 = str_λs[py_type](a_row)
            fmt_2 = F.format_string(a_row['s_format'], latinize(fmt_1)) # type: ignore
            return fmt_2

        fill_0 = { 
            0 : specs_df.index[specs_df['PyType'].isin(['int', 'dbl'])].tolist(), 
            '': specs_df.index[specs_df['PyType'] == 'str' ].tolist(), 
            date(1900, 1, 1): specs_df.index[specs_df['PyType'] == 'date'].tolist()}
        
        cast_1 = [F.col(rr['nombre']).cast(self.spk_types[rr['PyType']]()) 
            for _, rr in specs_df.iterrows()]
        
        strg_2 = [row_formatter(rr).alias(rr['nombre']) 
            for _, rr in specs_df.iterrows()]

        return {'0-fill': fill_0, '1-cast': cast_1, '2-string': strg_2}
            

    def specs_setup_0(self, path):
        # ['nombre', 'Longitud', 'Width', 'PyType', 'columna_valor'] 
        
        specs_0 = pd.read_feather(path)
        #    .set_index('nombre'))
        
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
                self.c_formats[rr['PyType']].format(rr['width_1'], rr['precision']) 
                for _, rr in df.iterrows()], 
            y_format = lambda df: df['x_format'].str.replace(r'\.\d*', '', regex=True),
            c_format = lambda df: df['y_format'].where(df['PyType'] == 'int', df['x_format']), 
            s_format = lambda df: ["%{}.{}s".format(wth, wth) for wth in df['width']])

        return specs_df


    def specs_reader_1(self, specs_df: pd_DF, tables_dict) -> dict: 
        # SPECS_DF: ['tabla', 'is_na']
        readers  = defaultdict(list)
        missing  = defaultdict(set)
        fix_vals = []
        
        for _, rr in specs_df.iterrows(): 
            # Reading and Missing
            r_type = rr['PyType']
            if rr['tabla'] in tables_dict: 
                if rr['columna_valor'] in tables_dict[rr['tabla']].columns: 
                    call_as = F.col(rr['columna_valor']).alias(rr['nombre'])

                    readers[rr['tabla']].append(call_as)
                else: 
                    # Fixing missing columns as NA. 
                    missing[rr['tabla']].add(rr['columna_valor'])
                    r_value = self.na_types[r_type]
                    r_col = (F.lit(r_value)
                        .cast(self.spk_types[r_type]())
                        .alias(rr['nombre']))
                    fix_vals.append(r_col)
            else: 
                if rr['is_na']: 
                    r_value = self.na_types[r_type]
                else: 
                    r_value = rr['columna_valor']
                r_col = (F.lit(r_value)
                    .cast(self.spk_types[r_type]())
                    .alias(rr['nombre']))
                fix_vals.append(r_col)

        result_specs = {
            'readers' : dict(readers), 
            'missing' : valmap(list, missing), 
            'fix_vals': fix_vals}
        return result_specs


    def master_join_2(self, spec_joins, specs_dict, tables_dict): 
        readers = specs_dict['readers']
    
        joiner = iter(spec_joins)
        key_0  = next(joiner)
        
        main_tbl = (tables_dict[key_0]
            .select(*readers[key_0], *spec_joins[key_0]))
        n_key = next(joiner, None)
        while n_key is not None: 
            next_tbl = (tables_dict[n_key]
                .select(*readers[n_key], *spec_joins[n_key]))
            main_tbl = main_tbl.join(next_tbl, how='left', 
                on=list(map(column_name, spec_joins[n_key])))
            n_key = next(joiner, None)
        
        master_tbl = main_tbl.select('*', *specs_dict['fix_vals'])
        return master_tbl


    def save_task_3(self, task, gold_path, gold_table): 
        now_time = dt.now(tz=timezone('America/Mexico_City'))
        now_hm = now_time.strftime('%Y-%m-%d_%H%M')
        now_00 = now_time.strftime('%Y-%m-%d_00')
        
        cyber_key, cyber_name = self.reports[task]
        report_dir     = f"{gold_path}/{cyber_name}/_spark/{now_00}"
        report_recent  = f"{gold_path}/recent/{cyber_key}.txt"
        report_history = f"{gold_path}/history/{cyber_name}/{cyber_key}_{now_hm}.txt"
        
        print(f"{now_hm}_{cyber_key}_{task}.txt")
        gold_table.save_as_file(report_dir, report_recent,  header=False)
        gold_table.save_as_file(report_dir, report_history, header=True)
        return report_history
    
          
    def set_defaults(self): 
        self.reports = {
            'sap_saldos'    : ('C8BD1374',  'core_balance' ), 
            'sap_estatus'   : ('C8BD1353',  'core_status'  ), 
            'sap_pagos'     : ('C8BD1343',  'core_payments'), 
            'fiserv_saldos' : ('C8BD10000', 'cms_balance'  ),
            'fiserv_estatus': ('C8BD10001', 'cms_status'   ), 
            'fiserv_pagos'  : ('C8BD10002', 'cms_payments' )}

        self.na_types = {
            'str' : '', 
            'dbl' : 0, 
            'int' : 0, 
            'long': 0,
            'date': date(1900, 1, 1)}

        self.spk_types = {
            'str' : T.StringType, 
            'dbl' : T.DoubleType, 
            'int' : T.IntegerType, 
            'long': T.LongType, 
            'date': T.DateType}

        self.c_formats = {
            'int' : '%0{}d', 
            'dbl' : '%0{}.{}f', 
            'dec' : '%0{}.{}d',  # Puede ser '%0{}.{}f'
            'str' : '%-{}.{}s', 
            'date': '%8.8d', 
            'long': '%0{}d'}



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

