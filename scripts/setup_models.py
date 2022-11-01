import os
from sqlalchemy import create_engine

from src.app.models import Base

from dotenv import load_dotenv
from config import PLATFORM_KEYS
load_dotenv(override=True)


sql_keys = PLATFORM_KEYS['local']['sqls']['hylia']

_env = (lambda key_like: 
    os.getenv(key_like[1]) if isinstance(key_like, tuple) else key_like)
key_dict = (lambda a_dict: 
    {k: _env(v) for (k, v) in a_dict.items()})


engine_str = ("mssql+pyodbc://{user}:{password}@{host}/{database}?driver={driver}"
    .format(**key_dict(sql_keys)))
hylia_engine = create_engine(engine_str, echo=True)

Base.metadata.create_all(hylia_engine)






