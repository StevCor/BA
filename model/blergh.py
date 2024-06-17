import sqlalchemy
from sqlalchemy import create_engine, text


def build_engine_to_connect_to_db(username, password, host, port, db_name, db_dialect):
    engine = None
    db_url = f'{username}:{password}@{host}:{str(port)}/{db_name}'

    if(db_dialect == 'MariaDB'):
        engine_url = f'mariadb+pymysql://{db_url}'
        print(engine_url)
        engine = create_engine(engine_url)
        connection = engine.connect()
        with engine.connect() as mdb_conn:
            print(mdb_conn.execute(text('SELECT * FROM studierende')))
    elif(db_dialect == 'PostgreSQL'):
        # PostgreSQL:
        engine_url = f'{db_dialect.lower()}://{db_url}'
        print(engine_url)
        postgres_engine = create_engine('postgresql://postgres:arc-en-ciel@localhost:5432/Test')
        connection = postgres_engine.connect()
        # Verbindungsaufbau taken from https://www.youtube.com/watch?v=yBDHkveJUf4
        with postgres_engine.connect() as pg_conn:
            result = pg_conn.execute(text('SELECT * FROM studierende'))
            print(result.all()) # result.all() ist eine Python-Liste -> Zugriff auf einzelne Elemente mit [x], einzelne Elemente sind 
            # sqlalchemy.engine.LegacyRows -> können mit dict(row) in Python-Dictionarys umgewandelt werden [Format: {'Spaltenname 1': 'Attributname'}]
            result_dicts = []
            for row in result.all():
                result_dicts.append(dict(row))
    else:
        print('Dieser SQL-Dialekt wird von diesem Tool nicht unterstützt.')
    
    return connection

def execute_db_query(engine, query):
    print(query)
    result_dicts = []
    with engine.connect() as conn:
        result = conn.execute(text(query))
        for row in result.all():
            result_dicts.append(dict(row))
        


build_engine_to_connect_to_db('Co', 'yo', 'localhost', 5432, 'Test', 'PostgreSQL')

# Durchführung von Datenbankabfragen (SELECT), Ausgabe des Ergebnisses 

# Ausführung von Änderungen an der Datenbank (UPDATE, DELETE), Ausgabe des Ergebnisses