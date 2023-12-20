
from sqlalchemy import text

from database_manager.smarter_data_manager import get_session_maker, get_dict_data, supabase


async def upload_table():
    to_table = input('Введите имя таблицы на сайте, которую хотите заполнить: ')
    db_url = input("Введите название базы данных (например, my_database.db): ")
    session_maker, engine = get_session_maker(f'sqlite+aiosqlite:///{db_url}')
    supabase.table(to_table).select('*').limit(1).execute()

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
