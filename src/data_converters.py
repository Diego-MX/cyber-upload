import pandas as pd
from pandas import (DataFrame as pd_DF, Series as pd_S)
from pyspark.sql import (functions as F, types as T, 
    Window as W, DataFrame as spk_DF, Column)
from typing import Union



class DataConverter(): 
    '''
    Given a source, use generalized procedures to trace its ETL methods
    as the source evolves from RAW -> BRZ -> SLV -> GLD. 
    '''
    
    def __init__(self, source): 
        self.source = source
        
        
    def specs_as_steps(self, specs = Union[str, pd_DF]) -> dict: 
        '''
        SPECS come in a more user-wise format.  
        For example, Excel tables with specified fields, or their corresponding Feather files. 
        STEPS is a Pipeline-like dictionary that has known elements.
        In the future, the return object is intended as a Pipelin. '''
        if isinstance(specs, str): 
            specs = pd.read_feather(specs)
            
        if self.source == 'cyber': 
            pass 
        if self.source == 'zendesk': 
            pass 
        return

    
    def brz_to_gold(self, brz_DF: spk_DF, specs_df: pd_DF) -> spk_DF: 
        pass 
    
    
    def delta_output(self, gld_DF: spk_DF, out_df: pd_DF) -> spk_DF: 
        '''Given a Δ table, apply specified steps to export it.'''
        
        if self.source == 'cyber': 
            pass
        if self.source == 'deltalake': 
            pass
        return 
        
        
        
@F.pandas_udf(T.StringType())
def udf_encode(x_str: pd_S, encoding='ascii', elementwise=True):   
    # Se consideró también ISO-8859-1 que es lo más parecido a ANSI/ASCII
    if elementwise: 
        λ_enc = lambda xx: xx.encode(encoding, 'ignore')
        y_str = x_str.str.normalize('NFKD').map(λ_enc) 
    else: 
        y_str = (normalize('NFKD', x_str)
            .encode(encoding, 'ignore')
            .decode(encoding))
    return y_str

    
