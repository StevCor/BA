from sqlalchemy import Engine, text
from model.databaseModel import get_primary_key_from_engine

def get_table_creation_information_from_engine(engine:Engine, table_name:str):
    query = f"SELECT ordinal_position, column_name, data_type, character_maximum_length, is_nullable, column_default FROM information_schema.columns WHERE table_name = '{table_name}'"
    if engine.dialect.name == 'postgresql':
        query = text(f"{query} AND table_catalog = '{engine.url.database}' ORDER BY ordinal_position")
    elif engine.dialect.name == 'mariadb':
        query = text(f"{query} AND table_schema = DATABASE() ORDER BY ordinal_position")
    else:
        print('Nicht implementiert.')
        return None
    with engine.connect() as connection:
        result = connection.execute(query)
    column_information = dict()
    for row in result:
        is_nullable = False
        if row[4] == 'YES':
            is_nullable = True
        column_information[row[1]] = {'data_type': row[2], 'max_length': row[3], 'nullable': is_nullable, 'default': row[5]}
    primary_key = get_primary_key_from_engine(engine, table_name)
    print(column_information, primary_key)
    return column_information, primary_key