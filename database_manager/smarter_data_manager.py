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
main_table_name = 'transportation_data'
metadata_obj = MetaData()
db_url = 'main_db.db'


def get_session_maker(url: str) -> tuple[async_sessionmaker, AsyncEngine]:
    engine: AsyncEngine = create_async_engine(url=url, pool_pre_ping=True, echo=True)
    return async_sessionmaker(engine, class_= AsyncSession), engine


def get_transportation_data() -> list[dict]:
    return supabase.table(main_table_name).select("*").execute().data


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


async def download_transportation_table_to_database():
    transportation_data = get_transportation_data()
    table_name = input('Введите имя таблицы с сайта: ')

    if not transportation_data:
        raise Exception("На сайте нет данных ёпта...")
    columns = generate_columns(transportation_data)

    transport_table = Table(
        table_name,
        metadata_obj,
        *columns
    )
    db_url = input("Введите название базы данных (например, my_database.db): ")
    session_maker, engine = get_session_maker(f'sqlite+aiosqlite:///{db_url}')

    async with engine.begin() as conn:
        res = await conn.execute(
            text(f'''SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}' '''))
        if res.first():
            raise Exception('Таблица уже загружена')
        await conn.run_sync(metadata_obj.create_all)


    async with engine.connect() as conn:
        for row in transportation_data:
            stmt = insert(transport_table).values(**row)
            await conn.execute(stmt)
        await conn.commit()

def get_dict_data(keys, records) -> list[dict]:
    data = []
    for record in records:
        dict_record = {}
        for i in range(len(record)):
            dict_record[keys[i]] = record[i]
        data.append(dict_record)
    return data
async def copy_table():
    session_maker, engine = get_session_maker(f'sqlite+aiosqlite:///{db_url}')
    async with engine.connect() as conn:
        original_table_name = input("Введите имя таблицы, которую хотите скопировать: ")
        res = await conn.execute(text(f'''SELECT name FROM sqlite_master WHERE type='table' AND name='{original_table_name}' '''))
        if not res.first():
            raise Exception("Таблица, которую вы пытаетесь скопировать, не существует")
        res = await conn.execute(text(f'''SELECT name FROM sqlite_master WHERE type='table' AND name='{original_table_name + '_copy'}' '''))
        if res.first():
            raise Exception("Вы уже копировали эту таблицу")

    async with engine.connect() as conn:
        res = await conn.execute(text(f'''SELECT * FROM "{original_table_name}"'''))
        records, keys = res.all(), list(res.keys())
    data = get_dict_data(keys, records)
    #print(data)
    copy_table = Table(original_table_name + '_copy', metadata_obj, *generate_columns(data))
    async with engine.begin() as conn:
        await conn.run_sync(metadata_obj.create_all)
    async with engine.connect() as conn:
        for row in data:
            stmt = insert(copy_table).values(**row)
            await conn.execute(stmt)
        await conn.commit()


async def upload_table():
    to_table = input('Введите имя таблицы на сайте, которую хотите заполнить: ')
    session_maker, engine = get_session_maker(f'sqlite+aiosqlite:///{db_url}')
    try:
        supabase.table(to_table).select('*').limit(1).execute()
    except:
        raise Exception('Most likely the table does not exist, check and try again')
    from_table = input('Введите имя таблицы, из которой хотите взять данные: ')
    async with engine.connect() as conn:
        res = await conn.execute(
        text(f'''SELECT name FROM sqlite_master WHERE type='table' AND name='{from_table}' '''))
        if not res.first():
            raise Exception("Таблица, из которой вы пытаетесь взять данные, не существует")
        res = await conn.execute(text(f'''SELECT * FROM "{from_table}"'''))
        records, keys = res.all(), list(res.keys())
    data = get_dict_data(keys, records)
    print(data)
    supabase.table(to_table).insert(data).execute()


async def main():
    #task1 = asyncio.create_task(download_transportation_table_to_database())
    #task2 = asyncio.create_task(copy_table())
    task3 = asyncio.create_task(upload_table())
    await task3
    #await task1
    #await task2

asyncio.run(main())

