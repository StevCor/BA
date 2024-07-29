from pandas import read_sql
from sqlalchemy import create_engine, text, select, column, bindparam
import urllib.parse
import re
from sqlalchemy.exc import OperationalError as operror
from sqlalchemy.exc import ArgumentError as argerror
from model.SQLDatabaseError import DatabaseError, QueryError, UpdateError

def connect_to_db(username:str, password:str, host:str, port:int, db_name:str, db_dialect:str, db_encoding:str):
    db_url = f'{username}:{urllib.parse.quote_plus(password)}@{host}:{str(port)}/{db_name}'
    engine_url = str()
    engine = None
    message = ''
    if(db_dialect == 'mariadb'):
        engine_url = f'{db_dialect}+pymysql://{db_url}?charset={db_encoding.lower()}'
    elif(db_dialect == 'postgresql'):
        engine_url = f'{db_dialect}://{db_url}'
    else:
        message = 'Dieser SQL-Dialekt wird von diesem Tool nicht unterstützt.'
        print(message)

    print(engine_url)
    # TODO: Vielleicht zu charset wechseln? https://stackoverflow.com/questions/45279863/how-to-use-charset-and-encoding-in-create-engine-of-sqlalchemy-to-create
    if db_dialect == 'mariadb':
        test_engine = create_engine(engine_url)
    else: 
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
        raise DatabaseError('Bitte überprüfen Sie den Servernamen, das Passwort sowie die Portnummer und versuchen es erneut.')
    except argerror: 
        raise DatabaseError('Bitte überprüfen Sie Ihre Angaben für den SQL-Dialekt und versuchen es erneut.') 
    except Exception as e:
        raise DatabaseError('Bitte überprüfen Sie Ihre Angaben und versuchen es erneut.')
    else:
        if db_dialect == 'mariadb':
            engine = create_engine(engine_url)
            # Ermöglicht die Verwendung von Anführungszeichen zur Kennzeichnung von als Identfier verwendeten Keywords und für Strings
            with engine.connect() as connection:
                connection.execute(text("SET sql_mode='ANSI_QUOTES'"))
                connection.commit()
        else:
            engine = create_engine(engine_url, connect_args = {'client_encoding': {db_encoding}})
    finally:
        try:
            connection.close()
        except UnboundLocalError:
            print('Keine Verbindung aufgebaut, daher auch kein Schließen nötig.')
    return engine

# def get_table_as_dataframe(engine, table_name):
#     table_name = convert_string_if_contains_capitals(table_name, engine.dialect.name)
#     table_df = None
#     primary_keys = get_primary_key_from_engine(engine, table_name)
#     query = text(f'SELECT * FROM {table_name}')
#     with engine.connect() as connection:
#         table_df = read_sql(sql = query, con = connection, index_col = primary_keys)
#     return table_df

# Ausgabe eines Dictionarys mit allen Tabellen- (Schlüssel) und deren Spaltennamen (als Liste), um sie in der Web-Anwendung anzeigen zu können
def list_all_tables_in_db(engine):
    table_names = dict()
    with engine.connect() as connection:
        result = None
        if engine.dialect.name == 'postgresql':
            result = connection.execute(text("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"))
        elif engine.dialect.name == 'mariadb':
            result = connection.execute(text("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE LIKE 'BASE_TABLE' AND TABLE_SCHEMA = DATABASE()"))
        else:
            return None
        for row in result:
            current_table = ''.join(tuple(row))
            query = f"SELECT column_name FROM information_schema.columns where table_name = '{current_table}'"
            if engine.dialect.name == 'postgresql':
                query = f"{query} AND table_catalog = '{engine.url.database}'"
            elif engine.dialect.name == 'mariadb':
                query = f"{query} AND table_schema = DATABASE()"
            columns_result = connection.execute(text(query))
            column_names = list()
            for column in columns_result:
                column = ''.join(tuple(column))
                column_names.append(column)
            table_names[current_table] = column_names
    # trotz with-Statement nötig, weil die Verbindung nicht geschlossen, sondern nur eine abgebrochene Transaktion rückgängig gemacht wird
    try:
        connection.close()
    except UnboundLocalError:
        print('Keine Verbindung erstellt.')
    print(table_names)
    return table_names

def get_full_table(engine, table_name:str):
    with engine.connect() as connection:
        result = connection.execute(text(f'SELECT * FROM {table_name}')) 
        return result   

def get_full_table_ordered_by_primary_key(engine, table_name:str, primary_keys:list):
    with engine.connect() as connection:
        result = connection.execute(text(f'SELECT * FROM {table_name} ORDER BY {primary_keys[0]}'))  
        return result

def search_string(engine, table_name:str, cols_and_dtypes:dict, string_to_search:str):
    primary_key = str()
    primary_keys = get_primary_key_from_engine(engine, table_name)
    for index, key in enumerate(primary_keys):
        if index == 0:
            primary_key = key
        else:
            primary_key = f'{primary_key}, {key}'
    table_name = convert_string_if_contains_capitals(table_name, engine.dialect.name)
    string_to_search = escape_string(engine.dialect.name, string_to_search)
    sql_condition = 'WHERE'
    for key in cols_and_dtypes.keys():
        attribute_to_search = convert_string_if_contains_capitals(key, engine.dialect.name).strip()
        if is_number(cols_and_dtypes[key], engine.dialect.name):
           attribute_to_search = f'CAST ({attribute_to_search} AS CHAR)'
        if sql_condition == 'WHERE':
            sql_condition = f"{sql_condition} {attribute_to_search} LIKE '%{string_to_search}%'"
        else:
            sql_condition = f"{sql_condition} OR {attribute_to_search} LIKE '%{string_to_search}%'"
        
    query = f"SELECT * FROM {table_name} {sql_condition}"

    # für den Fall, dass nur die Schlüssel und die durchsuchten Felder ausgegeben werden sollen:
    # if column_name in primary_key.split(','):
    #     query = f"SELECT {primary_key} FROM {table_name} WHERE {attribute_to_search} LIKE '%{string_to_search}%'"
    # else:
    #     query = f"SELECT {primary_key}, {column_name} FROM {table_name} WHERE {attribute_to_search} LIKE '%{string_to_search}%'"
    if engine.dialect.name == 'postgresql' or engine.dialect.name == 'mariadb':
        with engine.connect() as connection:
            print(text(query))
            result = connection.execute(text(query))
    else:
        print('Nicht implementiert.')
    return result

# alle Vorkommen eines Teilstrings in einer Spalte ersetzen
def replace_string_everywhere(engine, table_name, column_name:str, string_to_replace:str, replacement_string:str):
    table_name = convert_string_if_contains_capitals(table_name, engine.dialect.name)
    column_name = convert_string_if_contains_capitals(column_name, engine.dialect.name)
    string_to_replace = escape_string(engine.dialect.name, string_to_replace)
    replacement_string = escape_string(engine.dialect.name, replacement_string)
    query = ''
    if engine.dialect.name == 'postgresql':
        query = text(f"UPDATE {table_name} SET {column_name} = regexp_replace({column_name}, '(.*){string_to_replace}(.*)', '\\1{replacement_string}\\2');")
    elif engine.dialect.name == 'mariadb':
        query = text(f"UPDATE {table_name} SET {column_name} = regexp_replace({column_name}, '^(.*){string_to_replace}(.*)$', '\\\\1{replacement_string}\\\\2');")
        print(query)
    try:
        connection = engine.connect()
        connection.execute(query)
    except:
        connection.rollback()        
        raise UpdateError()
    else:
        connection.commit()
    finally:
        connection.close()

def replace_one_string(engine, table_name:str, column_name:str, string_to_replace:str, replacement_string:str, primary_keys_and_values:dict):
    table_name = convert_string_if_contains_capitals(table_name, engine.dialect.name)
    column_name = convert_string_if_contains_capitals(column_name, engine.dialect.name)
    if type(string_to_replace) == str:
        string_to_replace = escape_string(engine.dialect.name, string_to_replace)
    if type(replacement_string) == str:
        replacement_string = escape_string(engine.dialect.name, replacement_string)
    query = ''
    condition = build_sql_condition(tuple(primary_keys_and_values.keys()), 'AND')
    if engine.dialect.name == 'postgresql':
        query = f"UPDATE {table_name} SET {column_name} = regexp_replace({column_name}, '(.*){string_to_replace}(.*)', '\\1{replacement_string}\\2')"
    elif engine.dialect.name == 'mariadb':
        query = f"UPDATE {table_name} SET {column_name} = regexp_replace({column_name}, '^(.*){string_to_replace}(.*)$', '\\\\1{replacement_string}\\\\2')"
        print(query)
    query = text(f'{query} {condition};')
    for key in primary_keys_and_values.keys():
        query.bindparams(bindparam(key))
    try:
        connection = engine.connect() 
        connection.execute(query, primary_keys_and_values)
    except:
        raise UpdateError()
    else:
        connection.commit()
    finally:
        connection.close()

# # from https://gist.github.com/viewpointsa/bba4d475126ec4ef9427fd3c2fdaf5c1
# def copy_table(source_engine, connectionStringDst, table_name, verbose=False, condition="" ):
#     with source_engine.connect() as source_connection:
#         with psycopg2.connect(connectionStringDst) as connDst:
#             query = text(f'SELECT * FROM {convert_string_if_contains_capitals(table_name)} {condition}')
#             with source_connection.cursor() as source_cursor:                
#                 source_cursor.execute(query)
#                 print("Anzahl der Zeilen in der Ursprungstabelle: ", source_cursor.rowcount)
#                 with connDst.cursor() as curDst:
#                     for row in source_cursor:
#                         # generate %s x columns   
#                         for line in source_cursor.desription:
#                             query_columns = ','.join(line[0])
#                         query_values = ','.join('%s' for x in range(len(source_cursor.description)))
#                         query = f'INSERT INTO {convert_string_if_contains_capitals(table_name)} ({query_columns}) VALUES ({});'.format(table_name, query_columns, query_values)
#                         param = [ item for item in row ]
#                         if verbose:
#                             print curDst.mogrify(query,param )
#                         curDst.execute( query, param )



def update_to_unify_entries(engine, table_name:str, attribute_to_change:str, old_values:list, new_value:str, commit:bool):
    query = f'UPDATE {convert_string_if_contains_capitals(table_name, engine.dialect.name)} SET {convert_string_if_contains_capitals(attribute_to_change, engine.dialect.name)} = :new_value'
    cols_and_dtypes = get_column_names_and_datatypes_from_engine(engine, table_name)
    dtype_is_int_or_float = datatype_is_int_or_float(engine, cols_and_dtypes)
    condition_dict = {}
    print(old_values)
    for index, key in enumerate(cols_and_dtypes.keys()):
        if key == attribute_to_change:
            if dtype_is_int_or_float[key][1] == 1:
                new_value = int(new_value)
                for item in old_values:
                    item = int(item)
                break
            elif dtype_is_int_or_float[key][1] == 2:
                new_value = float(new_value)
                for item in old_values:
                    item = float(item)
                break 
    condition_dict['new_value'] = new_value
    condition = 'WHERE'
    for index, value in enumerate(old_values):
        if index == 0:
            condition = f'{condition} {convert_string_if_contains_capitals(attribute_to_change, engine.dialect.name)} = :value_{str(index)}'
        else:
            condition = f'{condition} OR {convert_string_if_contains_capitals(attribute_to_change, engine.dialect.name)} = :value_{str(index)}'
        condition_dict['value_' + str(index)] = value
    query = text(f'{query} {condition}')
    params = [new_value] + old_values
    print('UPDATE-Anweisung:', query)
    for key in condition_dict.keys():
        query.bindparams(bindparam(key))
    with engine.connect() as connection:
        result = connection.execute(query, condition_dict)
        if commit:
            connection.commit()
    return result

def datatype_is_int_or_float(engine, cols_and_dtypes:dict):
    dtype_is_int_or_float = {}
    for key in cols_and_dtypes.keys():
        item = cols_and_dtypes[key]
        if is_number(item, engine.dialect.name):
            if is_decimal_number(item):
                dtype_is_int_or_float[key] = [item, 2]
            else: 
                dtype_is_int_or_float[key] = [item, 1]
        else:
            dtype_is_int_or_float[key] = [item, 0]
    return dtype_is_int_or_float

def  get_row_number_of_affected_entries(engine, table_name:str, affected_attribute:str, primary_keys:list, old_values:list):
    key_for_ordering = primary_keys[0]
    condition = 'WHERE'
    condition_dict = {}
    cols_and_dtypes = get_column_names_and_datatypes_from_engine(engine, table_name)
    dtype_is_int_or_float = datatype_is_int_or_float(engine, cols_and_dtypes)

    for index, value in enumerate(old_values):
        if dtype_is_int_or_float[affected_attribute][1] == 0:
            condition_dict['value_' + str(index)] = value
        elif dtype_is_int_or_float[affected_attribute][1] == 1:
            condition_dict['value_' + str(index)] = int(value)
        elif dtype_is_int_or_float[affected_attribute][1] == 2:
            condition_dict['value_' + str(index)] = float(value)

        if index == 0:
            condition = f"{condition} sub.{convert_string_if_contains_capitals(affected_attribute, engine.dialect.name)} = :{'value_' + str(index)}"
        else:
            condition = f"{condition} OR sub.{convert_string_if_contains_capitals(affected_attribute, engine.dialect.name)} = :{'value_' + str(index)}"
    columns_to_select = '*'
    if engine.dialect.name == 'mariadb':
        for key in cols_and_dtypes.keys():
            if columns_to_select == '*':
                columns_to_select = key
            else:
                columns_to_select = f'{columns_to_select}, {key}'
    query = text(f"SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY {key_for_ordering}) AS Nr, {columns_to_select} FROM {table_name}) sub {condition}")
    print(query)
    for key in condition_dict.keys():
        query.bindparams(bindparam(key))
    with engine.connect() as connection:
        result = connection.execute(query, condition_dict)
    return result

def get_unique_values_for_attribute(engine, table_name, attribute_to_search:str):
    query = f'SELECT DISTINCT {attribute_to_search}, COUNT(*) AS Eintragsanzahl FROM {table_name} GROUP BY {attribute_to_search}'
    with engine.connect() as connection:
        result = connection.execute(text(query))
    return result

def get_column_names_and_datatypes_from_engine(engine, table_name):
    query = ''
    if engine.dialect.name == 'postgresql':
        query = text(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' AND table_catalog = '{engine.url.database}'")
    elif engine.dialect.name == 'mariadb':
        query = text(f"SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}'")
    else:
        print('Nicht implementiert.')
        return None
    with engine.connect() as connection:
        result = connection.execute(query)
    names_and_data_types = dict()
    for row in result:
        names_and_data_types[row[0]] = row[1]
    return names_and_data_types

def get_column_names_datatypes_and_number_type(engine, table_name):
    query = ''
    if engine.dialect.name == 'postgresql':
        query = text(f"SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '{table_name}' AND table_catalog = '{engine.url.database}'")
    elif engine.dialect.name == 'mariadb':
        query = text(f"SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, CHARACTER_MAXIMUM_LENGTH AS character_maximum_length FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}'")
    else:
        print('Nicht implementiert.')
        return None
    with engine.connect() as connection:
        result = connection.execute(query)
    names_and_data_types = dict()
    for row in result:
        print('Zeile: ', row)
        number_type = int()
        if is_number(row[1], engine.dialect.name):
            if is_decimal_number(row[1]):
                number_type = 2
            else:
                number_type = 1            
        else: 
            number_type = 0
        # nur für Textattribute vorhanden, None bei Zahlen
        char_max_length = row[2]
        names_and_data_types[row[0]] = [row[1], number_type, char_max_length]
    return names_and_data_types



def get_table_creation_information_from_engine(engine, table_name):
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


def get_primary_key_from_engine(engine, table_name:str):
    table_name = convert_string_if_contains_capitals(table_name, engine.dialect.name)
    with engine.connect() as connection:
        if engine.dialect.name == 'postgresql':
            primary_key_cursor = connection.execute(text(f"SELECT a.attname as column_name FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary"))
        elif engine.dialect.name == 'mariadb':
            primary_key_cursor = connection.execute(text(f"SELECT COLUMN_NAME as column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}' AND COLUMN_KEY = 'PRI'"))
    key_list = list()
    for column_name in primary_key_cursor:
        key_list.append(''.join(tuple(column_name)))
    print(key_list)
    return key_list

def get_primary_key_names_and_datatypes_from_engine(engine, table_name:str):
    with engine.connect() as connection:
        if engine.dialect.name == 'postgresql':
            primary_key_cursor = connection.execute(text(f"SELECT a.attname as column_name, format_type(a.atttypid, a.atttypmod) AS data_type FROM  pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary"))
        # Ergebnisse werden als Liste von Tupeln im Format '[('Primärschlüssel',)]' ausgegeben 
        elif engine.dialect.name == 'mariadb':
            primary_key_cursor = connection.execute(text(f"SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}' AND COLUMN_KEY = 'PRI'"))
    key_datatype_dict = dict()
    for item in primary_key_cursor:
        as_tuple = tuple(item)
        key_datatype_dict[as_tuple[0]] = as_tuple[1]
    print(key_datatype_dict)
    return key_datatype_dict

def get_row_count_from_engine(engine, table_name:str):
    with engine.connect() as connection:
        res = connection.execute(text(f'SELECT COUNT(1) FROM {table_name}'))
        return res.fetchone()[0] 

def build_update_query(table_name:str, column_names_of_affected_attributes:tuple, column_names_of_condition:tuple, values:tuple, operator:str):
    table_name = convert_string_if_contains_capitals(table_name, engine.dialect.name)
    if len(column_names_of_affected_attributes) + len(column_names_of_condition) != len(values):
        raise QueryError('Anzahl der Spaltennamen und Anzahl der Werte stimmen nicht überein.')
    query = f'UPDATE {table_name} SET'
    columns = ''
    for index, title in enumerate(column_names_of_affected_attributes):
        if len(column_names_of_affected_attributes) > 1 and index > 0:
            columns = f'{columns},  {convert_string_if_contains_capitals(title, engine.dialect.name)} = :{title}'
        else:
            columns = f'{columns}{convert_string_if_contains_capitals(title, engine.dialect.name)} = :{title}'
    condition = build_sql_condition(column_names_of_condition, operator)
    query = text(f'{query} {columns} {condition}')
    print(query)
    values_as_dict = dict(zip(column_names_of_affected_attributes + column_names_of_condition, values))
    print(values_as_dict)
    for key in values_as_dict.keys():
        print(key)
        print(values_as_dict[key])
        query.bindparams(bindparam(key))
        print(query)
    return query, values_as_dict

def build_sql_condition(column_names:tuple, operator:str = None):
    if (operator and operator.upper() not in ('AND', 'OR')) :
        raise QueryError('Der für die Bedingung angegebene Operator wird nicht unterstützt.')
    elif not operator and len(column_names) > 1:
        raise QueryError('Bei mehr als einem betroffenen Feld muss ein Operator für die Bedingung angegeben werden.')
    else:
        condition = 'WHERE'
        for index, item in enumerate(column_names):
            if len(column_names) > 1 and index > 0:
                condition = f'{condition} {operator.upper()}'
            condition = f'{condition} {convert_string_if_contains_capitals(item, engine.dialect.name)} = :{item}'
        return condition

def check_database_encoding(engine):
    encoding = ''
    if engine.dialect.name == 'postgresql':
        with engine.connect() as connection:
            res = connection.execute(text(f"SELECT pg_encoding_to_char(encoding) AS database_encoding FROM pg_database WHERE datname = '{engine.url.database}'"))
        if res.rowcount == 1:
            encoding = ''.join(res.one())
    elif engine.dialect.name == 'mariadb':
        with engine.connect() as connection:
            result = connection.execute(text("SHOW VARIABLES LIKE 'character_set_database'"))
        encoding = result.one()[1]
    return encoding

def is_number(data_type:str, db_dialect:str):
    if db_dialect == 'postgresql' and data_type.lower() in ('smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real', 'double precision', 'smallserial', 'serial', 'bigserial'):
        return True
    elif db_dialect == 'mariadb' and data_type.lower() in ('tinyint', 'boolean', 'int1', 'smallint', 'int2', 'mediumint', 'int3', 'int', 'integer', 'int4', 'bigint', 'int8', 'decimal', 'dec', 'numeric', 'fixed', 'float', 'double', 'double precision', 'real', 'bit'):
        return True    
    else:
        return False
    
def is_decimal_number(data_type:str):
    if data_type in ('decimal', 'dec', 'numeric', 'real', 'double precision', 'double', 'fixed', 'float'):
        return True
    else:
        return False

    
# setzt String in doppelte Anführungszeichen, wenn darin Großbuchstaben enthalten sind (nötig für Tabellen- und Spaltennamen in PostgreSQL)
def convert_string_if_contains_capitals(string:str, db_dialect:str):
    if db_dialect == 'postgresql' and any([x.isupper() for x in string]):
        string = f'"{string}"'
    return string
    
def escape_string(db_dialect:str, string):
    if db_dialect == 'postgresql':
        string = string.replace('%', '\%').replace('_', '\_').replace("'", "\'").replace('"', '\"').replace('\\', '\\\\')
    elif db_dialect == 'mariadb':
        string = string.replace('%', '\\%').replace('_', '\\_').replace("'", "\\'").replace('"', '\\"').replace('\\', '\\\\\\\\')
    return string

if __name__ == '__main__':
    engine = connect_to_db('postgres', 'arc-en-ciel', 'localhost', 5432, 'Test', 'postgresql', 'utf8')
    res = get_row_number_of_affected_entries(engine, 'punkte', 'punktzahl', ['matrikelnummer'], [20])
    for row in res:
        print(row)
    print('int' in 'integer')
    print(len('hello'))
