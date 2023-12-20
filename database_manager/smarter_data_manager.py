import asyncio


from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine, AsyncSession
from supabase import create_client, Client
from environs import Env
from sqlalchemy import MetaData, Table, Column, Integer, String, insert, text, REAL



env: Env = Env()
env.read_env()
url = env('BASE_URL')
apikey = env('apikey')
supabase: Client = create_client(url, apikey)
metadata_obj = MetaData()



def get_session_maker(url: str) -> tuple[async_sessionmaker, AsyncEngine]:
    engine: AsyncEngine = create_async_engine(url=url, pool_pre_ping=True, echo=True)
    return async_sessionmaker(engine, class_= AsyncSession), engine


def get_table_data(table_name) -> list[dict]:
    return supabase.table(table_name).select("*").execute().data


py_to_sqlite_types_mapping = {
    int: Integer,
    str: String,
    float: REAL
}


def generate_columns(data: list[dict]) -> list[Column]:
    columns = []
    for key, val in data[0].items():
        col = Column(key, py_to_sqlite_types_mapping[type(val)])
        columns.append(col)
    return columns



def get_dict_data(keys, records) -> list[dict]:
    data = []
    for record in records:
        dict_record = {}
        for i in range(len(record)):
            dict_record[keys[i]] = record[i]
        data.append(dict_record)
    return data



