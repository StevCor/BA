import sqlalchemy
from sqlalchemy import create_engine, text


def build_engine_to_connect_to_db(username, password, host, port, db_name, db_dialect):
    engine = None
    db_url = f'{username}:{password}@{host}:{str(port)}/{db_name}'
    engine_url = str()
    print(engine_url)
    if(db_dialect == 'MariaDB'):
        engine_url = f'mariadb+pymysql://{db_url}'
    
    elif(db_dialect == 'PostgreSQL'):
        # PostgreSQL:
        engine_url = f'{db_dialect.lower()}://{db_url}'
        print(engine_url)
        postgres_engine = create_engine('postgresql://postgres:arc-en-ciel@localhost:5432/Test?', connect_args={'client_encoding': 'utf8'})
        connection = postgres_engine.connect()
        # Verbindungsaufbau taken from https://www.youtube.com/watch?v=yBDHkveJUf4
        with postgres_engine.connect() as pg_conn:
            result = pg_conn.execute(text('SELECT * FROM studierende'))
            print(result.all()) # result.all() ist eine Python-Liste -> Zugriff auf einzelne Elemente mit [x], einzelne Elemente sind 
            # sqlalchemy.engine.LegacyRows -> können mit dict(row) in Python-Dictionarys umgewandelt werden [Format: {'Spaltenname 1': 'Attributname'}]
            result_dicts = []
            print(type(result.all()))

            for row in result.all():
                result_dicts.append(dict(row))
                # print(dict(row))
           # print(result_dicts)
    else:
        print('Dieser SQL-Dialekt wird von diesem Tool nicht unterstützt.')
    
    engine = create_engine(engine_url)
    return engine

if __name__ == '__main__':
    eng = build_engine_to_connect_to_db('Co', 'yo', 'localhost', 5432, 'Test', 'PostgreSQL')
    # execute_db_query(eng, 'SELECT * FROM studierende')

# Durchführung von Datenbankabfragen (SELECT), Ausgabe des Ergebnisses 

# Ausführung von Änderungen an der Datenbank (UPDATE, DELETE), Ausgabe des Ergebnisses

# Durchführung von Datenbankabfragen (SELECT), Ausgabe des Ergebnisses 

# Ausführung von Änderungen an der Datenbank (UPDATE, DELETE), Ausgabe des Ergebnisses