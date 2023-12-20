from sqlalchemy import text, Table, insert


from database_manager.smarter_data_manager import get_session_maker, get_dict_data, metadata_obj, generate_columns


async def copy_table():
    db_url = input("Введите название файла базы данных(например, example.db): ")
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