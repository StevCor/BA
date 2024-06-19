from sqlalchemy import create_engine, text, select, column
import urllib.parse

def build_engine_to_connect_to_db(username, password, host, port, db_name, db_dialect):
    db_url = f'{username}:{urllib.parse.quote_plus(password)}@{host}:{str(port)}/{db_name}'
    engine_url = str()
    if(db_dialect == 'MariaDB'):
        engine_url = f'mariadb+pymysql://{db_url}'
        print(engine_url)
        return create_engine(engine_url, connect_args={'client_encoding': 'utf8'})
 
    elif(db_dialect == 'PostgreSQL'):
        engine_url = f'{db_dialect.lower()}://{db_url}'
        print(engine_url)
        return create_engine(engine_url, connect_args={'client_encoding': 'utf8'})
        # result_dicts = []
        # # Verbindungsaufbau taken from https://www.youtube.com/watch?v=yBDHkveJUf4
        # with postgres_engine.connect() as pg_conn:
        #     result = pg_conn.execute(text('SELECT * FROM studierende'))
        #      # result.all() ist eine Python-Liste -> Zugriff auf einzelne Elemente mit [x], einzelne Elemente sind 
        #     # sqlalchemy.engine.LegacyRows -> können mit ._asdict(row) in Python-Dictionarys umgewandelt werden [Format: {'Spaltenname 1': 'Attributname'}]
        #     for row in result.all():
        #         print(row)
        #         result_dicts.append(row._asdict())
    else:
        print('Dieser SQL-Dialekt wird von diesem Tool nicht unterstützt.')
        return None
    
    
    # engine = create_engine(engine_url)
    # return engine

if __name__ == '__main__':
    engine = build_engine_to_connect_to_db('postgres', 'arc-en-ciel', 'localhost', 5432, 'Test', 'PostgreSQL')
    with engine.connect() as conn:
        result = conn.execute(text('SELECT * FROM studierende'))
        for row in result.all():
            print(row)


# Durchführung von Datenbankabfragen (SELECT), Ausgabe des Ergebnisses 

# Ausführung von Änderungen an der Datenbank (UPDATE, DELETE), Ausgabe des Ergebnisses

# Durchführung von Datenbankabfragen (SELECT), Ausgabe des Ergebnisses 

# Ausführung von Änderungen an der Datenbank (UPDATE, DELETE), Ausgabe des Ergebnisses