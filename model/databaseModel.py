from argparse import ArgumentError
import datetime
from sqlalchemy import CursorResult, Engine, create_engine, text, bindparam
import urllib.parse
from sqlalchemy.exc import OperationalError as operror
from sqlalchemy.exc import ArgumentError as argerror
from ControllerClasses import TableMetaData
from model.SQLDatabaseError import DatabaseError, DialectError, QueryError, UpdateError

def connect_to_db(username:str, password:str, host:str, port:int, db_name:str, db_dialect:str, db_encoding:str):
    db_url = f'{username}:{urllib.parse.quote_plus(password)}@{host}:{str(port)}/{urllib.parse.quote_plus(db_name)}'
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
    elif db_dialect == 'postgresql': 
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
        raise DatabaseError('Bitte überprüfen Sie den Datenbankbenutzernamen, den Servernamen sowie die Portnummer und versuchen es erneut.')
    except argerror: 
        raise DatabaseError('Bitte überprüfen Sie Ihre Angaben für den SQL-Dialekt und versuchen es erneut.') 
    except Exception as e:
        raise DatabaseError('Bitte überprüfen Sie Ihre Angaben und versuchen es erneut.')
    else:
        engine = test_engine
    finally:
        try:
            connection.close()
        except UnboundLocalError:
            print('Keine Verbindung aufgebaut, daher auch kein Schließen nötig.')
    return engine

def list_all_tables_in_db_with_preview(engine:Engine):
    table_names_and_columns = {}
    table_previews = {}
    query = ''
    if engine.dialect.name == 'postgresql':
        query = text("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'")
    elif engine.dialect.name == 'mariadb':
        query = text("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE LIKE 'BASE_TABLE' AND TABLE_SCHEMA = DATABASE()")
    else:
        return None
    result = execute_sql_query(engine, query)
    for row in result:
        current_table = ''.join(tuple(row))
        query = f'SELECT * FROM {convert_string_if_contains_capitals_or_spaces(current_table, engine.dialect.name)} LIMIT 20'
        preview_result = execute_sql_query(engine, text(query))
        column_names = list(preview_result.keys())
        preview_list = convert_result_to_list_of_lists(preview_result)
        table_names_and_columns[current_table] = column_names
        table_previews[current_table] = preview_list
    print(table_names_and_columns, table_previews)
    return table_names_and_columns, table_previews
 

def get_full_table_ordered_by_primary_key(table_meta_data:TableMetaData, convert:bool = True):
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, db_dialect)
    primary_keys = table_meta_data.primary_keys
    keys_for_ordering = ', '.join(primary_keys)
    query = text(f'SELECT * FROM {table_name} ORDER BY {keys_for_ordering}')
    if convert:
        return convert_result_to_list_of_lists(execute_sql_query(engine, query))
    else:
        return execute_sql_query(engine, query)




    



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









def get_column_names_data_types_and_max_length(engine:Engine, table_name:str):
    query = ''
    if engine.dialect.name == 'postgresql':
        query = text(f"SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = '{table_name}' AND table_catalog = '{engine.url.database}'")
    elif engine.dialect.name == 'mariadb':
        query = text(f"SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type, CHARACTER_MAXIMUM_LENGTH AS character_maximum_length FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}'")
    else:
        print('Nicht implementiert.')
        return None
    result = execute_sql_query(engine, query)
    column_names_and_data_types = dict()
    for row in result:
        column_name = row[0]
        data_type_lowercase = row[1].lower()
        data_type = row[1]
        # nur für Textattribute vorhanden, None bei Zahlen
        char_max_length = row[2]
        if engine.dialect.name == 'postgresql':
            text_int_decimal_or_other = 3
            if data_type_lowercase in ('character varying', 'varchar', 'character', 'char', 'bpchar', 'text'): 	
                text_int_decimal_or_other = 0
            elif data_type_lowercase in ('smallint', 'integer', 'bigint', 'smallserial', 'serial', 'bigserial'):
                text_int_decimal_or_other = 1
            elif data_type_lowercase in ('decimal', 'numeric', 'real', 'double precision'):
                text_int_decimal_or_other = 2
        elif engine.dialect.name == 'mariadb':
            text_int_decimal_or_other = 3
            if data_type_lowercase in ('binary', 'blob', 'char', 'char byte', 'enum', 'inet4', 'inet6', 'json', 'mediumblob', 'mediumtext', 'longblob', 'long', 'long varchar', 'longtext', 'row', 'text', 'tinyblob', 'tinytext', 'varbinary', 'varchar', 'set'): 	
                text_int_decimal_or_other = 0
            elif data_type_lowercase in ('tinyint', 'boolean', 'int1', 'smallint', 'int2', 'mediumint', 'int3', 'int', 'integer', 'int4', 'bigint', 'int8', 'bit'):
                text_int_decimal_or_other = 1
            elif data_type_lowercase in ('decimal', 'dec', 'numeric', 'fixed', 'float', 'double', 'double precision', 'real'):
                text_int_decimal_or_other = 2

       
        column_names_and_data_types[column_name] = {'data_type': data_type, 'data_type_group': text_int_decimal_or_other, 'char_max_length': char_max_length}
    return column_names_and_data_types







def get_row_count_from_engine(engine:Engine, table_name:str):
    table_name = convert_string_if_contains_capitals_or_spaces(table_name, engine.dialect.name)
    # https://datawookie.dev/blog/2021/01/sqlalchemy-efficient-counting/
    query = text(f'SELECT COUNT(1) FROM {table_name}')
    res = execute_sql_query(engine, query)
    return res.fetchone()[0] 

def build_sql_condition(column_names:tuple, db_dialect:str, operator:str = None):
    if (operator and operator.upper() not in ('AND', 'OR')) :
        raise QueryError('Der für die Bedingung angegebene Operator wird nicht unterstützt.')
    elif not operator and len(column_names) > 1:
        raise QueryError('Bei mehr als einem betroffenen Feld muss ein Operator für die Bedingung angegeben werden.')
    else:
        condition = 'WHERE'
        for index, item in enumerate(column_names):
            if len(column_names) > 1 and index > 0:
                condition = f'{condition} {operator.upper()}'
            condition = f'{condition} {convert_string_if_contains_capitals_or_spaces(item, db_dialect)} = :{item}'
        return condition

def check_database_encoding(engine:Engine):
    encoding = ''
    if engine.dialect.name == 'postgresql':
        with engine.connect() as connection:
            res = connection.execute(text(f"SELECT pg_encoding_to_char(encoding) AS database_encoding FROM pg_database WHERE datname = '{engine.url.database}'"))
        if res.rowcount == 1:
            encoding = ''.join(res.one())
    elif engine.dialect.name == 'mariadb':
        execute_sql_query
        # with engine.connect() as connection:
        #     result = connection.execute(text("SHOW VARIABLES LIKE 'character_set_database'"))
        query = text("SHOW VARIABLES LIKE 'character_set_database'")
        result = execute_sql_query(engine, query)
        encoding = result.one()[1]
    return encoding

def execute_sql_query(engine:Engine, query:text, params:dict = None, raise_exceptions:bool = False, commit:bool = None):
    result = None
    print(query)
    if params != None:
        for key in params.keys():
            query.bindparams(bindparam(key))
    try:
        connection = engine.connect()
        if engine.dialect.name == 'mariadb':
            connection.execute(text("SET sql_mode='ANSI_QUOTES'"))
            connection.commit()
        if params == None:
            result = connection.execute(query)
        else:
            result = connection.execute(query, params)
    except Exception as error:
        if raise_exceptions:
            print(type(error), str(error))
            raise error
        else:
            print(str(error))
            pass
    finally:
        try:
            if commit != None and commit:
                connection.commit()
            else:
                connection.rollback()
            connection.close()
        except UnboundLocalError:
            print('Keine Verbindung aufgebaut, daher auch kein Schließen nötig.')
    return result


   

def get_primary_key_from_engine(engine:Engine, table_name:str):
    table_name = convert_string_if_contains_capitals_or_spaces(table_name, engine.dialect.name)
    query = str()
    if engine.dialect.name == 'postgresql':
        # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
        query = text(f"SELECT a.attname as column_name FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary")
    elif engine.dialect.name == 'mariadb':
        query = text(f"SELECT COLUMN_NAME as column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}' AND COLUMN_KEY = 'PRI'")
    else: 
        print('Dialekt wird nicht unterstützt.')
        return None
    result = execute_sql_query(engine, query)

    key_list = list()
    for column_name in result:
        key_list.append(''.join(tuple(column_name)))
    print(key_list)
    return key_list
 
        


    
# setzt String in doppelte Anführungszeichen, wenn darin Leerzeichen oder Großbuchstaben enthalten sind (Letzteres ist ausschließlich für 
# Tabellen- und Spaltennamen in PostgreSQL nötig)
def convert_string_if_contains_capitals_or_spaces(string:str, db_dialect:str):
    if not string.startswith('"') and (db_dialect == 'postgresql' and any([x.isupper() for x in string])) or any([x == ' ' for x in string]):
        string = f'"{string}"'
    return string




def convert_result_to_list_of_lists(sql_result:CursorResult):
    result_list = [list(row) for row in sql_result.all()]
    return result_list  

def check_data_type_meta_data(engine:Engine, table_name:str):
    if not type(engine) == Engine or not type(table_name) == str :
        raise ArgumentError('Zum Ausführen dieser Funktion sind eine Engine und ein Tabellenname vom Typ String erforderlich.')
    db_dialect = engine.dialect.name
    result_dict = {}
    query = 'SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION'
    print('Dialekt: ', db_dialect)
    if db_dialect == 'mariadb':
        query = f'{query}, COLUMN_KEY, COLUMN_TYPE, EXTRA FROM information_schema.columns WHERE TABLE_SCHEMA = DATABASE()'
    elif db_dialect == 'postgresql':
        query = f"{query} FROM information_schema.columns WHERE TABLE_CATALOG = '{engine.url.database}'"
    else:
        raise DialectError('Dieser SQL-Dialekt wird nicht unterstüzt.')
    table_name = convert_string_if_contains_capitals_or_spaces(table_name, db_dialect)
    query = f"{query} AND TABLE_NAME = '{table_name}'"
   
    result = convert_result_to_list_of_lists(execute_sql_query(engine, text(query)))
    print('QUERY: ', query)
    # für PostgreSQL fehlt noch die Angabe, ob es sich um einen Primärschlüssel oder ein Attribut mit Unique-Constraint handelt
    if db_dialect == 'postgresql':
        constraint_query = f"SELECT a.attname FROM pg_constraint con JOIN pg_attribute a ON a.attnum = ANY(con.conkey) JOIN information_schema.columns i ON a.attname = i.column_name AND i.table_name = '{table_name}' WHERE con.conrelid = '{table_name}'::regclass AND con.conrelid = a.attrelid AND (con.contype = 'p' OR con.contype = 'u') AND i.table_catalog = '{engine.url.database}'"
        constraint_result = execute_sql_query(engine, text(constraint_query))
        unique_list = []
        for entry in constraint_result:
            if entry[0] not in unique_list:
                unique_list.append([table_name, entry[0]])
    for row in result:
        print('ROW. ', row)
        column_name = row[0]
        column_default = row[1]
        if row[2] == 'YES':
            is_nullable = True
        else:
            is_nullable = False                
        data_type = row[3]
        if (db_dialect == 'mariadb' and row[10] is not None and 'auto_increment' in row[10]) or (db_dialect == 'postgresql' and column_default is not None and 'nextval' in column_default):
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
        if datetime_precision != None:
            result_dict[column_name] = {'data_type_group': 'date', 'data_type': data_type, 'datetime_precision': datetime_precision}
        elif character_max_length != None:
            result_dict[column_name] = {'data_type_group': 'text', 'data_type': data_type, 'character_max_length': character_max_length}
        elif (db_dialect == 'mariadb' and 'tinyint(1)' in row[9].lower()) or (db_dialect == 'postgresql' and data_type == 'boolean'):
                result_dict[column_name] = {'data_type_group': 'boolean', 'data_type': 'boolean'}        
        elif numeric_precision != None:
            if 'int' in data_type or numeric_scale == 0:
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
            else:
                result_dict[column_name] = {'data_type_group': 'decimal', 'data_type': data_type, 'numeric_precision': numeric_precision, 'numeric_scale': numeric_scale}
        else: 
            result_dict[column_name] = {'data_type_group': data_type, 'data_type': data_type}
        if is_unsigned != None:
            result_dict[column_name]['is_unsigned'] = is_unsigned
        result_dict[column_name]['is_nullable'] = is_nullable
        result_dict[column_name]['is_unique'] = is_unique
        result_dict[column_name]['auto_increment'] = auto_increment
    return result_dict

if __name__ == '__main__':
    engine = connect_to_db('postgres', 'arc-en-ciel', 'localhost', 5432, 'Test', 'postgresql', 'utf8')
    yo = get_full_table_ordered_by_primary_key(engine, 'studierende', ['matrikelnummer'])
    for line in yo:
        print('line: ', line)
    maria_engine = connect_to_db('root', 'arc-en-ciel', 'localhost', 3306, 'test', 'mariadb', 'utf8')
    res = maria_engine.connect().execute(text("SELECT * FROM information_schema.columns WHERE TABLE_NAME = 'studierende';"))
    for row in res:
        print(row)
    # res = replace_all_string_occurrences(engine, 'studierende', ['matrikelnummer'], cols, '2', '23')
    # for row in res:
    #     print(row)
    # # print(get_replacement_information(engine, 'studierende', 'all', cols, ['matrikelnummer'], 'A', 'a'))
    # query = text('UPDATE studierende SET matrikelnummer = :new_value WHERE matrikelnummer = 5248796')
    # params = {'new_value': '55'}
    # for key in params.keys():
    #     query.bindparams(bindparam(key))
    # res = engine.connect().execute(query, params)

    result = engine.connect().execute(text(f"SELECT punktzahl FROM punkte WHERE punktzahl = '70' OR CAST(punktzahl AS CHAR) LIKE '%' || '70' || '%'"))
    for row in result:
        print('ROW: ', row)
    print('unsigned' in 'bigint(20) unsigned')


