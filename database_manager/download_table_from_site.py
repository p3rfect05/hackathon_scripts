from sqlalchemy import Table, text, insert

from database_manager.smarter_data_manager import generate_columns, metadata_obj, get_table_data, \
    get_session_maker


async def download_table_to_database():

    table_name = input('Введите имя таблицы с сайта: ')
    try:
        data = get_table_data(table_name)
    except:
        raise Exception('Похоже таблицы не существует')
    if not data:
        raise Exception("На сайте нет данных ёпта...")
    columns = generate_columns(data)

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
        for row in data:
            stmt = insert(transport_table).values(**row)
            await conn.execute(stmt)
        await conn.commit()
