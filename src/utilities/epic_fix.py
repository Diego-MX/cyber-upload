from datetime import date
from delta.tables import DeltaTable as Δ
from functools import reduce
from pyspark.sql import (functions as F, DataFrame as spk_DF, 
    SparkSession as spk_Sn, GroupedData, Column)
import re
from typing import Union

from epic_py.tools import MatchCase
from epic_py.delta import (EpicDF, EpicGroupDF, 
        as_column, column_name, when_plus)




