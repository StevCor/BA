# Modul für Datenbankoperationen, die für die Arbeit mit einer oder zwei Tabellen benötigt werden

from argparse import ArgumentError
from sqlalchemy import CursorResult, Engine, create_engine, text, bindparam
import urllib.parse
from sqlalchemy.exc import OperationalError as operror
from sqlalchemy.exc import ArgumentError as argerror
from ControllerClasses import TableMetaData
from model.SQLDatabaseError import DatabaseError, DialectError, QueryError, UpdateError


def connect_to_db(user_name:str, password:str, host:str, port:int, db_name:str, db_dialect:str, db_encoding:str):
    """Erstellung einer sqlalchemy.Engine für den Datenbankzugriff und Testen der Verbindung
    
    user_name: Datenbankbenutzername als String

    password: Datenbankpasswort als String

    host: Datenbankservername als String

    port: Datenbankserverportnummer als Intger

    db_name: Name der Datenbank als String

    db_dialect: SQL-Dialekt der Datenbank als String (aktuell 'mariadb' oder 'postgresql' erlaubt)

    db_encoding: Zeichencodierung der Datenbank (aktuell 'utf8' oder 'latin-1' erlaubt)

    Ausgabe der erstellten Engine, None, wenn keine Verbindung aufgebaut werden konnte; DialectError bei nicht unterstütztem SQL-Dialekt
    """
    # Aufbau der URL für die Engine
    db_url = f'{user_name}:{urllib.parse.quote_plus(password)}@{host}:{str(port)}/{urllib.parse.quote_plus(db_name)}'
    engine_url = str()
    engine = None
    # Für MariaDB muss dem Dialekt für die Vervollständigung des Treibernamens noch '+pymysql' angehängt werden, die Zeichencodierung wird als 
    # Parameter hinter einem Fragezeichen an die URL angehängt.
    if(db_dialect == 'mariadb'):
        engine_url = f'{db_dialect}+pymysql://{db_url}?charset={db_encoding.lower()}'
    # Für PostgreSQL werden der Dialektname und die URL nur zusammengefügt.
    elif(db_dialect == 'postgresql'):
        engine_url = f'{db_dialect}://{db_url}'
    else:
        # Bei nicht unterstützten Dialekten wird eine Fehlermeldung ausgegeben.
        raise DialectError('Dieser SQL-Dialekt wird von diesem Tool nicht unterstützt.')
        
    # Für MariaDB erfolgt die Erstellung der Engine mithilfe der URL
    if db_dialect == 'mariadb':
        test_engine = create_engine(engine_url)
    # Für PostgreSQL muss die Zeichencodierung in Form eines Dictionarys als Parameter übergeben werden.
    elif db_dialect == 'postgresql': 
        test_engine = create_engine(engine_url, connect_args = {'client_encoding': {db_encoding}})

    # Testen der Verbindung zur Sicherstellung, dass die erstellte Engine gültig ist
    # mögliche Fehler:
    # psycopg2.OperationalError bei falschen Angaben für Servername, Portnummer oder Encoding
    # UnicodeDecodeError bei falschen Benutzernamen
    # sqlalchemy.exc.ArgumentError bei falschem Dialekt
    # UnboundLocalError (im finally-Block) bei falschem Passwort oder falschem Benutzernamen  
    try:
        connection = test_engine.connect()
    except UnicodeDecodeError:
        raise DatabaseError('Bitte überprüfen Sie Ihren Benutzernamen, das Passwort und den Datenbanknamen und versuchen es erneut.')
    except operror:
        raise DatabaseError('Bitte überprüfen Sie den Datenbankbenutzernamen, den Servernamen sowie die Portnummer und versuchen es erneut.')
    except argerror: 
        raise DatabaseError('Bitte überprüfen Sie Ihre Angaben für den SQL-Dialekt und versuchen es erneut.') 
    except Exception:
        raise DatabaseError('Bitte überprüfen Sie Ihre Angaben und versuchen es erneut.')
    else:
        # Ohne Fehler wird die erstellte Engine übernommen, ...
        engine = test_engine
    finally:
        try:
            # ... anschließend wird die Verbindung geschlossen.
            connection.close()
        except UnboundLocalError:
            # Wenn keine Verbindung aufgebaut werden konnte, muss auch keine geschlossen werden.
            pass
    # Zuletzt wird überprüft, ob die angegebene Zeichencodierung mit der tatsächlichen Zeichencodierung der Datenbank übereinstimmt
    encoding = check_database_encoding(engine)
    # Ist dies nicht der Fall, wird die Engine nochmal mit der tatsächlichen Codierung erstellt
    if encoding.lower() != db_encoding.lower():
        if(db_dialect == 'mariadb'):
            engine = create_engine(f'{db_dialect}+pymysql://{db_url}?charset={encoding.lower()}')
        elif(db_dialect == 'postgresql'):
            engine = create_engine(f'{db_dialect}://{db_url}', connect_args = {'client_encoding': {encoding}})
    # Ausgabe der erstellten Engine
    return engine

def list_all_tables_in_db_with_preview(engine:Engine):
    """Beziehen der Daten für die Erstellung einer Vorschau aller Tabellen in der Datenbank einer Engine
    
    engine: sqlalchemy.Engine, über die der Datenbankzugriff erfolgt
    
    Gibt ein Dictionary mit den Tabellennamen als Schlüsseln und einer Liste der Attributnamen als Wert, ein Dictionary mit den Tabellennamen 
    als Schlüsseln und den ersten 20 Einträgen der Tabelle als Wert sowie eine Liste der Tabellen ohne Primärschlüssel aus.
    
    Ausgabe eines DialectErrors bei nicht unterstützten SQL-Dialekten"""
    # Anlegen der Rückgabevariablen
    table_names_and_columns = {}
    table_previews = {}
    query = ''
    # Die Abfragen für die existierenden Tabellen beruhen in PostgreSQL und MariaDB auf unterschiedlichen Servertabellen
    if engine.dialect.name == 'postgresql':
        query = text("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'")
    elif engine.dialect.name == 'mariadb':
        query = text("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE LIKE 'BASE_TABLE' AND TABLE_SCHEMA = DATABASE()")
    else:
        raise DialectError(f'Der SQL-Dialekt {engine.dialect.name} wird nicht unterstützt.')
    # Ausführen der Abfrage
    result = execute_sql_query(engine, query)
    # Anlegen einer leeren Liste für die Tabellen ohne Primärschlüssel
    tables_without_keys = []
    for row in result:
        # Die Abfrage liefert nur den Tabellennamen, jedoch muss dieser zur Weiterverarbeitung noch in einen einfachen String umgewandelt werden.
        current_table = ''.join(tuple(row))
        # Beziehen der Primärschlüsselattributnamen der aktuellen Tabelle
        primary_keys = get_primary_key_from_engine(engine, current_table)
        # Wenn die dabei erhaltene Liste leer ist, hat die aktuelle Tabelle keine Primärschlüsselattribute, ...
        if len(primary_keys) == 0:
            # ... daher wird sie in die entsprechende Liste eingetragen.
            tables_without_keys.append(current_table)
        # Anschließend werden die ersten 20 Tupel der aktuellen Tabelle abgefragt.
        query = f'SELECT * FROM {convert_string_if_contains_capitals_or_spaces(current_table, engine.dialect.name)} LIMIT 20'
        preview_result = execute_sql_query(engine, text(query))
        # Beziehen der Attributnamen aus den Schlüsseln des CursorResults
        column_names = list(preview_result.keys())
        # Umwandlung des Vorschauergebnisses in eine Liste von Listen
        preview_list = convert_result_to_list_of_lists(preview_result)
        # Einfügen der Attributnamen in das entsprechende Ausgabe-Dictionary
        table_names_and_columns[current_table] = column_names
        # Einfügen der Vorschau in das entsprechende Ausgabe-Dictionary
        table_previews[current_table] = preview_list
    return table_names_and_columns, table_previews, tables_without_keys
 
def get_full_table_ordered_by_primary_key(table_meta_data:TableMetaData, convert:bool = True):
    """Beziehen einer vollständigen Tabelle, nach den Primärschlüsselattributen geordnet.
    
    table_meta_data: TableMetaData-Objekt der Tabelle
    
    convert: Boolean-Wert; wenn True, wird das Abfrageergebnis als Liste von Listen ausgegeben, sonst als CursorResult.
    
    Ausgabe der Tabelle als CursorResult oder als Liste von Listen; Ausgabe eines DialectErrors bei nicht unterstützten SQL-Dialekten."""
    # Beziehen der benötigten Variablen aus dem TableMetaData-Objekt der Tabelle
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    primary_keys = table_meta_data.primary_keys
    # Ausgabe des Fehlers bei nicht unterstütztem Dialekt
    if db_dialect != 'mariadb' and db_dialect != 'postgresql':
        raise DialectError(f'Der SQL-Dialekt {db_dialect} wird nicht unterstützt.')
    # Umwandlung des Tabellennamens, falls dieser Großbuchstaben (PostgreSQL) oder Leerzeichen (beide) enthält
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, db_dialect)
    # Umwandlung der Primärschlüsselliste in einen String, ggf. mit Escaping
    keys_for_ordering = ', '.join([convert_string_if_contains_capitals_or_spaces(key, db_dialect) for key in primary_keys])
    # Erstellen der Abfrage
    query = text(f'SELECT * FROM {table_name} ORDER BY {keys_for_ordering}')
    # Ausführung der Abfrage, ...
    # ... entweder mit ...
    if convert:
        return convert_result_to_list_of_lists(execute_sql_query(engine, query))
    # ... oder ohne Umwandlung in eine Liste von Listen.
    else:
        return execute_sql_query(engine, query)

def get_row_count_from_engine(engine:Engine, table_name:str):
    """Ermittlung der Gesamtanzahl von Tupeln in einer Tabelle.
    
    engine: sqlalchemy.Engine mit Zugriff auf die entsprechende Datenbank

    table_name: Name der abzufragenden Tabelle als String.

    Ausgabe der Gesamttupelanzahl als Integer; Ausgabe eines DialectErrors bei nicht unterstützten SQL-Dialekten.
    """
    # Ausgabe des Fehlers für nicht unterstützte Dialekte
    if engine.dialect.name not in ('mariadb', 'postgresql'):
        raise DialectError(f'Der SQL-Dialekt {engine.dialect.name} wird nicht unterstützt.')
    # Umwandlung des Tabellennamens
    table_name = convert_string_if_contains_capitals_or_spaces(table_name, engine.dialect.name)
    # Abfrage übernommen von https://datawookie.dev/blog/2021/01/sqlalchemy-efficient-counting/
    query = text(f'SELECT COUNT(1) FROM {table_name}')
    # Ausführen der Abfrage
    res = execute_sql_query(engine, query)
    # Ausgabe des Wertes 
    return res.fetchone()[0] 

def build_sql_condition(column_names:tuple, db_dialect:str, operator:str = None):
    """Aufbau einer SQL-Abfragen-Bedingung
    
    column_names: Tupel mit den zu berücksichtigenden Attributnamen als Strings
    
    db_dialect: SQL-Dialekt der Datenbank, an die die Abfrage gesendet wird

    operator: gewünschter Operator als String ('AND' oder 'OR' erlaubt).

    Ausgabe der SQL-Bedingung als String; Ausgabe eines QueryErrors, wenn ungültige Operatoren oder zu wenige Attribute angegeben sind oder 
    bei mehr als einem Attribut der Operator fehlt.
    """
    # Überprüfung, ob die Argumente geeignet sind
    if (operator and operator.upper() not in ('AND', 'OR')):
        raise QueryError('Der für die Bedingung angegebene Operator wird nicht unterstützt.')
    elif len(column_names) < 1:
        raise QueryError('Bitte geben Sie mindestens ein Attribut für die Bedingung an.')
    elif not operator and len(column_names) > 1:
        raise QueryError('Bei mehr als einem betroffenen Feld muss ein Operator für die Bedingung angegeben werden.')
    # Aufbau der Bedingung
    condition = 'WHERE'
    for index, item in enumerate(column_names):
        # Wenn mehr als ein Attribut angegeben ist und es sich schon um min. die zweite Iteration handelt, ...
        if len(column_names) > 1 and index > 0:
            # ... wird der Bedingung der Operator angehangen.
            condition = f'{condition} {operator.upper()}'
        # Anschließend wird der Bedingung das ggf. mit Anführungszeichen versehene Attribut angehängt, das mit einem Platzhalter gleichgesetzt wird,
        # der beim Ausführen der Abfrage mit dem konkreten Wert ersetzt wird.
        condition = f'{condition} {convert_string_if_contains_capitals_or_spaces(item, db_dialect)} = :{item}'
    return condition

def check_database_encoding(engine:Engine):
    """Beziehen der Datenbank-Zeichencodierung aus den Servertabellen
    
    engine: sqlchlemy.Engine mit Zugriff auf die aktuelle Datenbank
    
    Ausgabe der Zeichencodierung als String; bei nicht unterstütztem SQL-Dialekt ein DialectError."""

    encoding = ''
    if engine.dialect.name == 'postgresql':
        result = execute_sql_query(engine, text(f"SELECT pg_encoding_to_char(encoding) AS database_encoding FROM pg_database WHERE datname = '{engine.url.database}'"))
        if result.rowcount == 1:
            encoding = ''.join(result.one())
    elif engine.dialect.name == 'mariadb':
        query = text("SHOW VARIABLES LIKE 'character_set_database'")
        result = execute_sql_query(engine, query)
        encoding = result.one()[1]
    else:
        raise DialectError(f'Der SQL-Dialekt {engine.dialect.name} wird nicht unterstützt.')
    return encoding

def execute_sql_query(engine:Engine, query:text, params:dict = None, raise_exceptions:bool = False, commit:bool = None):
    """Ausführung vorgefertigter SQL-Abfragen

    engine: sqlalchemy.Engine mit Zugriff auf die gewünschte Datenbank 
    
    query: SQL-Abfrage als sqlalchemy.text
    
    params: optionales Dictionary mit den Namen und Werten der bei der Abfrage zu berücksichtigenden Parameter
    
    raise_exceptions: optionaler Boolean-Wert, standardmäßig False. Wenn True, werden auftretende Exceptions weitergegeben
    
    commit: optionaler Boolean-Wert. Die Abfrage (UPDATE o. Ä.) wird nur in die Datenbank geschrieben, wenn dieser Wert auf True gesetzt ist.
    
    Ausgabe des CursorResults der Abfrage oder None"""

    result = None
    # Wenn Parameter angegeben sind, ...
    if params != None:
        for key in params.keys():
            # ... müssen diese zunächst an die Abfrage gebunden werden.
            query.bindparams(bindparam(key))
    try:
        # Aufbau der Verbindung
        connection = engine.connect()
        # Für MariaDB: Setzen der Trennzeichen auf einfache Anführungszeichen, damit einfache Abfragen in beiden Dialekten gleich gehandhabt werden können
        if engine.dialect.name == 'mariadb':
            connection.execute(text("SET sql_mode='ANSI_QUOTES'"))
            # Speichern des Ergebnisses
            connection.commit()
        # Ohne Parameter wird die Abfrage allein ausgeführt, ...
        if params is None:
            result = connection.execute(query)
        else:
            # ... wenn Parameter angegeben sind, müssen diese bei der Ausführung übergeben werden.
            result = connection.execute(query, params)
    # Abfangen von Fehlern
    except Exception as error:
        # Wenn gewünscht, werden sie weitergereicht, ...
        if raise_exceptions:
            raise error
        else:
            # ... anderenfalls werden sie ignoriert.
            pass
    finally:
        try:
            # Schreiben der Anfrage in die Datenbank, wenn gewünscht
            if commit != None and commit:
                connection.commit()
            # Anderenfalls wird die Transaktion rückgängig gemacht
            else:
                connection.rollback()
            # Zuletzt wird die Verbindung geschlossen
            connection.close()
        # Hierbei auftretende UnboundLocalErrors können ignoriert werden, weil bei nicht aufgebauten Verbindungen auch kein Schließen nötig ist. 
        except UnboundLocalError:
            pass
    # Ausgabe des Ergebnisses bzw. None bei Fehlern
    return result



def get_primary_key_from_engine(engine:Engine, table_name:str):
    """Beziehen der Primärschlüsselattribute einer Tabelle aus den Servertabellen
    
    engine: sqlalchemy.Engine mit Zugriff auf die gewünschte Datenbank
    
    table_name: Name der abzufragenden Tabelle als String
    
    Ausgabe der Primärschlüsselattributnamen als Liste; DialectError bei nicht unterstützten SQL-Dialekten."""

    # Escaping des Tabellennamens
    table_name = convert_string_if_contains_capitals_or_spaces(table_name, engine.dialect.name)
    query = ''
    ### Erstellen und Ausführen der Abfrage ###
    if engine.dialect.name == 'postgresql':
        # Abfrage übernommen von https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        query = text(f"SELECT a.attname as column_name FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary")
    elif engine.dialect.name == 'mariadb':
        query = text(f"SELECT COLUMN_NAME as column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}' AND COLUMN_KEY = 'PRI'")
    else: 
        raise DialectError(f'Der SQL-Dialekt {engine.dialect.name} wird nicht unterstützt.')
    result = execute_sql_query(engine, query)
    ### Erstellen der Primärschlüsselliste aus dem Abfrageergebnis ###
    key_list = list()
    for column_name in result:
        key_list.append(''.join(tuple(column_name)))
    return key_list
 
        


    

def convert_string_if_contains_capitals_or_spaces(string:str, db_dialect:str):
    """Setzt einen String in doppelte Anführungszeichen, wenn darin Leerzeichen oder Großbuchstaben enthalten sind (Letzteres ist ausschließlich für 
    Tabellen- und Spaltennamen in PostgreSQL nötig).
    
    string: zu betrachtende Zeichenkette
    
    db_dialect: SQL-Dialekt der betroffenen Datenbank.
    
    Ausgabe des ggf. mit doppelten Anführungszeichen umgebenen Strings, sonst des unveränderten Strings."""

    if (not string.startswith('"') and not string.endswith('"')) and (db_dialect == 'postgresql' and any([x.isupper() for x in string])) or any([x == ' ' for x in string]):
        string = f'"{string}"'
    return string




def convert_result_to_list_of_lists(sql_result:CursorResult):
    """Umwandlung eines CursorResults in eine Liste von Listen (Matrix), damit das Ergebnis ggf. mehrfach durchiteriert werden kann.
    
    sql_result: CursorResult, Ergebnis der Datenbankabfrage
    
    Ausgabe einer Liste von Listen."""

    result_list = [list(row) for row in sql_result.all()]
    return result_list  

def get_data_type_meta_data(engine:Engine, table_name:str):
    """Beziehen der Datentypinformationen (Attributname, Standardwert, Datentyp, max. Länge, Eindeutigkeit, NULL-Wert-Toleranz) aus den Servertabellen
    
    engine: sqlalchemy.Engine mit Zugriff auf die gewünschte Tabelle
    
    table_name: Name der abzufragenden Tabelle als String
    
    Ausgabe eines Dictionarys (mit Attributnamen als Schlüsseln) von Dictionarys mit den Datentypinformationen; bei falschen Argumentdatentypen Ausgabe
    eines ArgumentErrors."""
    # Argumentüberprüfung
    if not type(engine) == Engine or not type(table_name) == str :
        raise ArgumentError(None, 'Zum Ausführen dieser Funktion sind eine Engine und ein Tabellenname vom Typ String erforderlich.')
    db_dialect = engine.dialect.name
    result_dict = {}
    # Die ersten 8 abzufragenden Attribute sind für MariaDB und PostgreSQL gleich
    query = 'SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION'
    # In MariaDB können aus der gleichen Tabelle noch Informationen dazu bezogen werden, ob das Attribut ein Primärschlüssel ist (COLUMN_KEY),
    # mit welchem Ausdruck es in der Tabellenerstellung angelegt werden würde (COLUMN_TYPE) und Kommentaren oder Standardwerten (EXTRA)
    if db_dialect == 'mariadb':
        query = f'{query}, COLUMN_KEY, COLUMN_TYPE, EXTRA FROM information_schema.columns WHERE TABLE_SCHEMA = DATABASE()'
    elif db_dialect == 'postgresql':
        query = f"{query} FROM information_schema.columns WHERE TABLE_CATALOG = '{engine.url.database}'"
    else:
        raise DialectError('Dieser SQL-Dialekt wird nicht unterstüzt.')
    # Die Bedingung für den Tabellennamen ist wieder gleich
    query = f"{query} AND TABLE_NAME = '{table_name}'"
   
    result = convert_result_to_list_of_lists(execute_sql_query(engine, text(query)))
    table_name = convert_string_if_contains_capitals_or_spaces(table_name, db_dialect)
    
    # Für PostgreSQL fehlt noch die Angabe, ob es sich um einen Primärschlüssel oder ein Attribut mit UNIQUE-Constraint handelt
    if db_dialect == 'postgresql':
        constraint_query = f"SELECT a.attname FROM pg_constraint con JOIN pg_attribute a ON a.attnum = ANY(con.conkey) JOIN information_schema.columns i ON a.attname = i.column_name AND i.table_name = '{table_name}' WHERE con.conrelid = '{table_name}'::regclass AND con.conrelid = a.attrelid AND (con.contype = 'p' OR con.contype = 'u') AND i.table_catalog = '{engine.url.database}'"
        constraint_result = execute_sql_query(engine, text(constraint_query))
        # Bündelung der Attribute mit einzigartigen Werten in einer Liste
        unique_list = []
        for entry in constraint_result:
            if entry[0] not in unique_list:
                unique_list.append([table_name, entry[0]])
    ##### Umwandlung des Abfrageergebnisses in das Datentypmetadaten-Dictionary #####
    for row in result:
        ### Auslesen der Werte ###
        column_name = row[0]
        column_default = row[1]
        if row[2] == 'YES':
            is_nullable = True
        else:
            is_nullable = False                
        data_type = row[3]
        # Auto-Inkrementierung steht für MariaDB im Attribut 'EXTRA', für PostgreSQL enthält der Standardwert in einem solchen Fall den Ausdruck 'nextval'
        # mit Verweis auf eine (hier irrelevante) Sequenz
        if (db_dialect == 'mariadb' and row[10] is not None and 'auto_increment' in str(row[10]).lower()) or (db_dialect == 'postgresql' and column_default is not None and 'nextval' in column_default):
            auto_increment = True
        else:
            auto_increment = False
        character_max_length = row[4]
        numeric_precision = row[5]
        numeric_scale = row[6]
        datetime_precision = row[7]
        if (db_dialect == 'mariadb' and (row[8] == 'PRI' or row[8] == 'UNI')) or (db_dialect == 'postgresql' and [table_name, column_name] in unique_list):
            is_unique = True
        else:
            is_unique = False  
        if (db_dialect == 'mariadb' and 'unsigned' in row[9].lower()) or (db_dialect == 'postgresql' and (data_type == 'boolean' or 'serial' in data_type)):
            is_unsigned = True
        else:
            is_unsigned = None
        result_dict[column_name] = {}
        # Für Datumsdatentypen ist neben der Datentypgruppe und dem Datentyp die Datetime-Precision relevant
        if datetime_precision != None:
            result_dict[column_name] = {'data_type_group': 'date', 'data_type': data_type, 'datetime_precision': datetime_precision}
        # Für textbasierte Datentypen wird die max. erlaubte Zeichenanzahl ausgelesen
        elif character_max_length != None:
            result_dict[column_name] = {'data_type_group': 'text', 'data_type': data_type, 'character_max_length': character_max_length}
        # Booleans werden in MariaDB als 'tinyint(1)' hinterlegt
        elif (db_dialect == 'mariadb' and 'tinyint(1)' in row[9].lower()) or (db_dialect == 'postgresql' and data_type == 'boolean'):
                result_dict[column_name] = {'data_type_group': 'boolean', 'data_type': 'boolean'}      
        # Wenn numeric_precision nicht NULL ist, handelt es sich bei dem betroffenen Wert um eine Zahl 
        elif numeric_precision != None:
            # Ganze Zahlen können am Datentypteil 'int' oder an einem Wert von 0 für die numeric_scale erkannt werden
            if 'int' in data_type or numeric_scale == 0:
                # Serial ist ein Alias, diese Datentypen werden intern als Arten von Integer behandelt
                if db_dialect == 'mariadb' and data_type == 'bigint' and is_unsigned and not is_nullable and auto_increment and is_unique:
                    data_type = 'serial'
                elif db_dialect == 'postgresql' and auto_increment:
                    if data_type == 'bigint':
                        data_type = 'bigserial'
                    elif data_type == 'integer':
                        data_type = 'serial'
                    elif data_type == 'smallint':
                        data_type = 'smallserial'
                result_dict[column_name] = {'data_type_group': 'integer', 'data_type': data_type, 'numeric_precision': numeric_precision}
            # Anderenfalls handelt es sich um eine Dezimalzahl, für die numeric_precision und numeric_scale relevant sind
            else:
                result_dict[column_name] = {'data_type_group': 'decimal', 'data_type': data_type, 'numeric_precision': numeric_precision, 'numeric_scale': numeric_scale}
        # Für alle anderen Datentypen wird lediglich der Datentyp übernommen; einmal als Datentypgruppe, einmal als Datentyp.
        else: 
            result_dict[column_name] = {'data_type_group': data_type, 'data_type': data_type}
        # In MariaDB können vorzeichenlose Zahlen bestehen, füge hierzu einen eigenen Wert in das Dictionary ein.
        if is_unsigned != None:
            result_dict[column_name]['is_unsigned'] = is_unsigned
        # Übernahme der Werte, ob das Attribut den Wert NULL annehmen darf, des Standardwertes, einer bestehenden UNIQUE-Constraint und der Auto-Inkrementierung
        result_dict[column_name]['is_nullable'] = is_nullable
        result_dict[column_name]['column_default'] = column_default
        result_dict[column_name]['is_unique'] = is_unique
        result_dict[column_name]['auto_increment'] = auto_increment
    return result_dict