from sqlalchemy import create_engine, text, select, column
import urllib.parse
from psycopg2 import OperationalError
from sqlalchemy.exc import OperationalError as operror
from sqlalchemy.exc import ArgumentError as argerror
from model.SQLDatabaseError import DatabaseError

def build_engine_to_connect_to_db(username, password, host, port, db_name, db_dialect, db_encoding):
    db_url = f'{username}:{urllib.parse.quote_plus(password)}@{host}:{str(port)}/{db_name}'
    engine_url = str()
    engine = None
    message = ''
    if(db_dialect == 'mariadb'):
        engine_url = f'{db_dialect}+pymysql://{db_url}'
    elif(db_dialect == 'postgresql'):
        engine_url = f'{db_dialect}://{db_url}'
    else:
        message = 'Dieser SQL-Dialekt wird von diesem Tool nicht unterstützt.'
        print(message)

    print(engine_url)
    # Mit 'utf8' anstelle der Variablen db_encoding hat es funktioniert
    # TODO: Vielleicht zu charset wechseln? https://stackoverflow.com/questions/45279863/how-to-use-charset-and-encoding-in-create-engine-of-sqlalchemy-to-create
    test_engine = create_engine(engine_url, connect_args = {'client_encoding': {db_encoding}})

    # Verbindung testen - mögliche Fehler:
    # psycopg2.OperationalError bei falschen Angaben für Servername, Portnummer oder Encoding
    # UnicodeDecodeError bei falschen Benutzernamen
    # sqlalchemy.exc.ArgumentError bei falschem Dialekt
    # UnboundLocalError (im finally-Block) bei falschem Passwort oder falschem Benutzernamen  
    try:
        connection = test_engine.connect()
    except UnicodeDecodeError:
        raise DatabaseError('Bitte überprüfen Sie Ihren Benutzernamen, das Passwort und den Datenbanknamen und versuchen es erneut.')
    except operror:
        print('Kein Verbindungsaufbau möglich!')
        raise DatabaseError('Bitte überprüfen Sie den Servernamen sowie die Portnummer und versuchen es erneut.')
    except argerror: 
        raise DatabaseError('Bitte überprüfen Sie Ihre Angaben für den SQL-Dialekt und versuchen es erneut.') 
    except Exception as e:
        raise DatabaseError('Bitte überprüfen Sie Ihre Angaben und versuchen es erneut.')
    else:
        engine = create_engine(engine_url, connect_args = {'client_encoding': {db_encoding}})
    finally:
        try:
            connection.close()
        except UnboundLocalError:
            print('Keine Verbindung aufgebaut, daher auch kein Schließen nötig.')
 
    

        # result_dicts = []
        # # Verbindungsaufbau taken from https://www.youtube.com/watch?v=yBDHkveJUf4
        # with postgres_engine.connect() as pg_conn:
        #     result = pg_conn.execute(text('SELECT * FROM studierende'))
        #      # result.all() ist eine Python-Liste -> Zugriff auf einzelne Elemente mit [x], einzelne Elemente sind 
        #     # sqlalchemy.engine.LegacyRows -> können mit ._asdict(row) in Python-Dictionarys umgewandelt werden [Format: {'Spaltenname 1': 'Attributname'}]
        #     for row in result.all():
        #         print(row)
        #         result_dicts.append(row._asdict())
    return engine

# Ausgabe eines Dictionarys mit allen Tabellen- (Schlüssel) und deren Spaltennamen (als Liste), um sie in der Web-Anwendung anzeigen zu können
def list_all_tables_in_db(engine):
    table_names = dict()
    with engine.connect() as connection:
        if engine.dialect.name == 'postgresql':
            result = connection.execute(text("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"))
            for row in result:
                current_table = str(row[1])
                columns_result = connection.execute(text(f"SELECT column_name FROM information_schema.columns where table_name = '{current_table}'"))
                column_names = list()
                for column in columns_result:
                    column = str(column).removeprefix('(\'').removesuffix('\',)')
                    column_names.append(column)
                table_names[current_table] = column_names
        elif engine.dialect.name == 'mariadb':
            #TODO
            print('Not implemented yet.')
    # trotz with-Statement nötig, weil die Verbindung nicht geschlossen, sondern nur eine abgebrochene Transaktion rückgängig gemacht wird
    try:
        connection.close()
    except UnboundLocalError:
        print('Keine Verbindung erstellt.')
    print(table_names)
    return table_names        
    
        

if __name__ == '__main__':
    try:
        engine = build_engine_to_connect_to_db('postgres', 'arc-en-ciel', 'localhost', 54332, 'Test', 'postgresql', 'utf8')[0]
    except OperationalError:
        print('Not working.')
    except DatabaseError as error:
        engine = None
        print(error)
    try:
        print(list_all_tables_in_db(engine))
        # conn = engine.connect()
        # result = conn.execute(text("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"))
        # for row in result.all():
        #     print(row)
    except Exception as error:
        print(error)

        


# Durchführung von Datenbankabfragen (SELECT), Ausgabe des Ergebnisses 

# Ausführung von Änderungen an der Datenbank (UPDATE, DELETE), Ausgabe des Ergebnisses

# Durchführung von Datenbankabfragen (SELECT), Ausgabe des Ergebnisses 

# Ausführung von Änderungen an der Datenbank (UPDATE, DELETE), Ausgabe des Ergebnisses