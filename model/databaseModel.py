from sqlalchemy import CursorResult, Engine, create_engine, text, bindparam
import urllib.parse
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
        if db_dialect == 'mariadb':
            # Ermöglicht die Verwendung von Anführungszeichen zur Kennzeichnung von als Identfier verwendeten Keywords und für Strings
            with engine.connect() as connection:
                # https://database.guide/7-options-for-enabling-pipes-as-the-concatenation-operator-in-mariadb/
                connection.execute(text("SET sql_mode='POSTGRESQL';"))
                #connection.execute(text("SET sql_mode='ANSI_QUOTES'"))
                connection.commit()
    finally:
        try:
            connection.close()
        except UnboundLocalError:
            print('Keine Verbindung aufgebaut, daher auch kein Schließen nötig.')
    return engine

# Ausgabe eines Dictionarys mit allen Tabellen- (Schlüssel) und deren Spaltennamen (als Liste), um sie in der Web-Anwendung anzeigen zu können
def list_all_tables_in_db(engine:Engine):
    table_names = dict()
    query = ''
    if engine.dialect.name == 'postgresql':
        query = text("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'")
    elif engine.dialect.name == 'mariadb':
        query = text("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE LIKE 'BASE_TABLE' AND TABLE_SCHEMA = DATABASE()")
    else:
        return None
    # with engine.connect() as connection:
    #     result = connection.execute(query)
    result = execute_sql_query(engine, query)
    for row in result:
        current_table = ''.join(tuple(row))
        query = f"SELECT column_name FROM information_schema.columns where table_name = '{current_table}'"
        if engine.dialect.name == 'postgresql':
            query = f"{query} AND table_catalog = '{engine.url.database}'"
        elif engine.dialect.name == 'mariadb':
            query = f"{query} AND table_schema = DATABASE()"
        # columns_result = connection.execute(text(query))
        columns_result = execute_sql_query(engine, text(query))
        column_names = list()
        for column in columns_result:
            column = ''.join(tuple(column))
            column_names.append(column)
        table_names[current_table] = column_names
    # trotz with-Statement nötig, weil die Verbindung nicht geschlossen, sondern nur eine abgebrochene Transaktion rückgängig gemacht wird
    # try:
    #     connection.close()
    # except UnboundLocalError:
    #     print('Keine Verbindung erstellt.')
    print(table_names)
    return table_names

def get_full_table(engine:Engine, table_name:str):
    query = text(f'SELECT * FROM {table_name}')
    return convert_result_to_list_of_lists(execute_sql_query(engine, query))   

def get_full_table_ordered_by_primary_key(engine:Engine, table_name:str, primary_keys:list, convert:bool = True):
    table_name = convert_string_if_contains_capitals(table_name, engine.dialect.name)
    keys_for_ordering = ', '.join(primary_keys)
    query = text(f'SELECT * FROM {table_name} ORDER BY {keys_for_ordering}')
    if convert:
        return convert_result_to_list_of_lists(execute_sql_query(engine, query))
    else:
        return execute_sql_query(engine, query)

def search_string(engine:Engine, table_name:str, cols_and_dtypes:dict, string_to_search:str):
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
    operator, cast_data_type = set_operator_and_cast_data_type(engine.dialect.name)
    for key in cols_and_dtypes.keys():
        attribute_to_search = convert_string_if_contains_capitals(key, engine.dialect.name).strip()
        if cols_and_dtypes[key]['data_type_group'] != 0:
            attribute_to_search = f'CAST ({attribute_to_search} AS {cast_data_type})'
        if sql_condition == 'WHERE':
            sql_condition = f"{sql_condition} {attribute_to_search} {operator} '%{string_to_search}%'"
        else:
            sql_condition = f"{sql_condition} OR {attribute_to_search} {operator} '%{string_to_search}%'"
        
    query = text(f"SELECT * FROM {table_name} {sql_condition}")

    # für den Fall, dass nur die Schlüssel und die durchsuchten Felder ausgegeben werden sollen:
    # if column_name in primary_key.split(','):
    #     query = f"SELECT {primary_key} FROM {table_name} WHERE {attribute_to_search} LIKE '%{string_to_search}%'"
    # else:
    #     query = f"SELECT {primary_key}, {column_name} FROM {table_name} WHERE {attribute_to_search} LIKE '%{string_to_search}%'"
    if engine.dialect.name == 'postgresql' or engine.dialect.name == 'mariadb':
        # with engine.connect() as connection:
        #     result = connection.execute(query)
        result = convert_result_to_list_of_lists(execute_sql_query(engine, query))
    else:
        print('Nicht implementiert.')
    return result

def get_replacement_information(engine:Engine, table_name:str, affected_attributes_and_positions:list, cols_dtypes_and_numbertypes:dict, primary_keys:list, old_value:str, replacement:str):
    if len(affected_attributes_and_positions) != len(cols_dtypes_and_numbertypes.keys()):
        raise QueryError('Für alle Attribute der Tabelle muss angegeben sein, ob sie von der Änderung betroffen sein können oder nicht.')
    affected_attributes = []
    positions = []
    for item in affected_attributes_and_positions:
        print('item: ', item)
        if item[1]:
            affected_attributes.append(item[0])
        positions.append(item[1])
        
    occurrence_dict = {}
    if len(affected_attributes) > 1:
        unaltered_table = get_full_table_ordered_by_primary_key(engine, table_name, primary_keys, convert = False)
        all_attributes = list(unaltered_table.keys())
        primary_key_indexes = []
        for index, key in enumerate(all_attributes):
            if key in primary_keys:
                primary_key_indexes.append(index)
        unaltered_table = convert_result_to_list_of_lists(unaltered_table)
        table_with_full_replacement = replace_all_string_occurrences(engine, table_name, affected_attributes, cols_dtypes_and_numbertypes, old_value, replacement, commit = False)
        row_nos_old_and_new_values = get_indexes_of_affected_attributes_for_replacing(engine, table_name, cols_dtypes_and_numbertypes, primary_keys, old_value, affected_attributes)
        occurrence_counter = 0
        primary_key_value = []
        for row_no in row_nos_old_and_new_values.keys():
            old_values = list(unaltered_table[row_no-1])
            positions = row_nos_old_and_new_values[row_no]
            for index in positions:
                if index != 0:
                    occurrence_counter += 1
                    for pk_index in primary_key_indexes:
                        primary_key_value.append(old_values[pk_index])
                    occurrence_dict[occurrence_counter] = {'row_no': row_no, 'primary_key': primary_key_value, 'affected_attribute': all_attributes[index]}
            new_values = list(table_with_full_replacement[row_no-1])
            for index in range(len(old_values)):
                if not positions[index]:
                    new_values[index] = None
            row_nos_old_and_new_values[row_no] = {'old': old_values, 'positions': positions, 'new': new_values, 'primary_key': primary_key_value}
    elif len(affected_attributes) == 1:
        attribute_with_full_replacement = replace_all_string_occurrences(engine, table_name, affected_attributes, cols_dtypes_and_numbertypes, old_value, replacement, commit = False)
        affected_row_nos_and_unaltered_entries = get_row_number_of_affected_entries(engine, table_name, cols_dtypes_and_numbertypes, affected_attributes, primary_keys, [old_value], 'replace', convert = False)
        row_nos_old_and_new_values = dict()
        affected_column_no = None
        occurrence_counter = 0
        for row in attribute_with_full_replacement:
            print('This is it: ', row)
        for index, key in enumerate(affected_row_nos_and_unaltered_entries.keys()):
            print('KEY:', key)
            if key == affected_attributes[0]:
                affected_column_no = index - 1
                break
        for row in affected_row_nos_and_unaltered_entries:
            primary_key_value = []
            for index, key in enumerate(affected_row_nos_and_unaltered_entries.keys()):
                if key in primary_keys:
                    primary_key_value.append(row[index])
            print('ROW: ', row)
            row_no = row[0]
            old_values = list(row[1:])
            new_values = [None] * len(old_values)
            print(affected_column_no)
            new_values[affected_column_no] = attribute_with_full_replacement[row_no-1][0]
            print(primary_key_value)
            row_nos_old_and_new_values[row_no] = {'old': old_values, 'positions': positions, 'new': new_values, 'primary_key': primary_key_value}
            occurrence_counter += 1
            occurrence_dict[occurrence_counter] = {'row_no': row_no, 'primary_key': primary_key_value, 'affected_attribute': affected_attributes[0]}
    else:
        raise QueryError('Es muss mindestens ein Attribut angegeben sein, dessen Werte bearbeitet werden sollen.')
    return row_nos_old_and_new_values, occurrence_dict
            
            
            
    


def get_indexes_of_affected_attributes_for_replacing(engine:Engine, table_name:str, cols_dtypes_and_numbertypes:dict, primary_keys:list, old_value:str, affected_attributes:list = None):
    table_name = convert_string_if_contains_capitals(table_name, engine.dialect.name)
    string_to_replace = escape_string(engine.dialect.name, old_value)
    params_dict = {'old_value': string_to_replace}
    keys = ', '.join(primary_keys)
    query = 'SELECT'
    case_selected_attribute = 'THEN 1 ELSE 0 END'
    case_nonselected_attribute = 'CASE WHEN 0 = 0 THEN 0 END'
    operator, cast_data_type = set_operator_and_cast_data_type(engine.dialect.name)
    condition = f"{operator} '%' || :old_value || '%'"
    for index, key in enumerate(cols_dtypes_and_numbertypes.keys()):
        if affected_attributes == None or (affected_attributes != None and key in affected_attributes):
            if cols_dtypes_and_numbertypes[key]['data_type_group'] != 0: #[1]
                query = f'{query} CASE WHEN CAST({key} AS {cast_data_type}) {condition} {case_selected_attribute}'
            else:
                query = f'{query} CASE WHEN {key} {condition} {case_selected_attribute}'
        else: 
            query = f'{query} {case_nonselected_attribute}'
        if index < len(cols_dtypes_and_numbertypes.keys())-1:
            query = f'{query},'
    query = text(f'{query} FROM {table_name} ORDER BY {keys}')
    print(query)
    result = execute_sql_query(engine, query, params_dict)

    row_ids = dict()
    for index, row in enumerate(result):
        if sum(row) != 0:
            # index + 1, da die SQL-Funktion ROW_NUMBER() ab 1 zählt; Listenkonversion, damit die Einträge verändert werden können
            row_ids[index+1] = list(row)

    # Variante mit Ausgabe aller Attribute & Indizes
    # primary_key_indexes = []
    # for index, key in enumerate(result.keys()):
    #     if key in primary_keys:
    #         primary_key_indexes.append(index)
    # print('PKs: ', primary_key_indexes)
    # key_list = []
    # for index, row in enumerate(result):
    #     print(row)

    #     item_index = 0
    #     while item_index < len(row)/2:
    #         if item_index in primary_key_indexes:
    #             key_list.append(row[item_index])
    #         item_index += 1

    #     row = row[len(row)//2:]
    #     if sum(row) != 0:
    #         row_ids[tuple(key_list)] = list(row)
    #     key_list = []
    return row_ids

# alle Vorkommen eines Teilstrings ersetzen
def replace_all_string_occurrences(engine:Engine, table_name:str, column_names:list, cols_and_dtypes:dict, string_to_replace:str, replacement_string:str, commit:bool = False):
    table_name = convert_string_if_contains_capitals(table_name, engine.dialect.name)
    string_to_replace = escape_string(engine.dialect.name, string_to_replace)
    replacement_string = escape_string(engine.dialect.name, replacement_string)
    primary_keys = ', '.join(get_primary_key_from_engine(engine, table_name))
    update_params = {}
    flag = ''
    if engine.dialect.name == 'postgresql':
        flag = ", 'g'"
    query = f'UPDATE {table_name} SET'
    for index, column_name in enumerate(column_names):
        column_name = convert_string_if_contains_capitals(column_name, engine.dialect.name)
        is_text_int_float_or_other = cols_and_dtypes[column_name]['data_type_group']
        if is_text_int_float_or_other != 0:
            query = f"{query} {column_name} = :new_value_{str(index)}"
            if is_text_int_float_or_other == 1:
                update_params[f'new_value_{str(index)}'] = int(replacement_string)
            elif is_text_int_float_or_other == 2:
                update_params[f'new_value_{str(index)}'] = float(replacement_string)
            elif is_text_int_float_or_other == 3:
                update_params[f'new_value_{str(index)}'] = replacement_string
        else:
            query = f"{query} {column_name} = regexp_replace({column_name}, :string_to_replace, :replacement_string{flag})"
            if 'string_to_replace' not in update_params.keys():
                update_params['string_to_replace'] = string_to_replace
                update_params['replacement_string'] = replacement_string
        if column_name != column_names[len(column_names)-1]:
            query = f'{query},' 
    print(query)
    query = text(query)
    for key in update_params.keys():
        query.bindparams(bindparam(key))
    try:
        connection = engine.connect()
        connection.execute(query, update_params)
        if len(column_names) == 1 and commit == False:
            result = connection.execute(text(f'SELECT {column_names[0]} FROM {table_name} ORDER BY {primary_keys}'))
        else:
            result = connection.execute(text(f'SELECT * FROM {table_name} ORDER BY {primary_keys}'))
    except Exception as error:
        connection.rollback()        
        raise UpdateError(str(error))
    else:
        if commit:
            connection.commit()
        else:
            connection.rollback()
    finally:
        try:
            connection.close()
        except Exception as error:
            raise error
    return convert_result_to_list_of_lists(result)

def replace_some_string_occurrences(engine:Engine, table_name:str, cols_and_dtypes:dict, occurrences_dict:dict, string_to_replace:str, replacement_string:str, commit:bool = False):
    table_name = convert_string_if_contains_capitals(table_name, engine.dialect.name)
    string_to_replace = escape_string(engine.dialect.name, string_to_replace)
    replacement_string = escape_string(engine.dialect.name, replacement_string)
    primary_key_attributes = occurrences_dict[0]['primary_keys']
    occurrences_dict.pop(0)
    success_counter = 0
    failed_updates = []
    for row in occurrences_dict.values():
        print(row)
        query = f'UPDATE {table_name} SET'
        update_params = {}
        primary_key = row['primary_key']
        flag = ''
        if engine.dialect.name == 'postgresql':
            flag = ", 'g'"
        if len(primary_key) != len(primary_key_attributes):
            raise QueryError('Es müssen gleich viele Spaltennamen und Attributwerte für den Primärschlüssel angegeben werden.')
        affected_attribute = row['affected_attribute']
        is_text_int_float_or_other = cols_and_dtypes[affected_attribute]['data_type_group']
        if is_text_int_float_or_other != 0:
            query = f"{query} {affected_attribute} = :new_value"
            if is_text_int_float_or_other == 1:
                update_params[f'new_value'] = int(replacement_string)
            elif is_text_int_float_or_other == 2:
                update_params[f'new_value'] = float(replacement_string)
            elif is_text_int_float_or_other == 3:
                update_params[f'new_value'] = replacement_string
        else:
            query = f"{query} {affected_attribute} = regexp_replace({affected_attribute}, :string_to_replace, :replacement_string{flag})"
            update_params['string_to_replace'] = string_to_replace
            update_params['replacement_string'] = replacement_string
        for index, key in enumerate(primary_key_attributes):
            update_params[key] = primary_key[index]
        condition = build_sql_condition(tuple(primary_key_attributes), engine.dialect.name, 'AND')
        query = text(f'{query} {condition}')
        print(query)
        try:
            execute_sql_query(engine, query, update_params, raise_exceptions = True, commit = commit)
        except Exception:
            failed_updates.append(primary_key)
        else:
            success_counter += 1
    if success_counter == len(occurrences_dict):
        if len(occurrences_dict) == 1:
            return f'Der ausgewählte Wert wurde erfolgreich aktualisiert.'
        else:
            return f'Alle {len(occurrences_dict)} ausgewählten Werte wurden erfolgreich aktualisiert.'
    else:
        if success_counter == 1:
            verb = 'wurde'
        else:
            verb = 'wurden'
        return f'{success_counter} von {len(occurrences_dict)} betroffenen Werten {verb} erfolgreich aktualisiert. Fehler sind in den Zeilen mit folgenden Primärschlüsselwerten aufgetreten: {failed_updates}. Bitte sehen Sie sich diese nochmal an.'

    

def replace_one_string(engine:Engine, table_name:str, column_name:str, string_to_replace:str, replacement_string:str, primary_keys_and_values:dict):
    table_name = convert_string_if_contains_capitals(table_name, engine.dialect.name)
    column_name = convert_string_if_contains_capitals(column_name, engine.dialect.name)
    if type(string_to_replace) == str:
        string_to_replace = escape_string(engine.dialect.name, string_to_replace)
    if type(replacement_string) == str:
        replacement_string = escape_string(engine.dialect.name, replacement_string)
    query = ''
    condition = build_sql_condition(tuple(primary_keys_and_values.keys()), engine.dialect.name, 'AND')
    flag = ''
    if engine.dialect.name == 'postgresql': 
        flag = ", 'g'"       
    query = f"UPDATE {table_name} SET {column_name} = regexp_replace({column_name}, '{string_to_replace}', '{replacement_string}'{flag})"
    print(query)
    query = text(f'{query} {condition};')
    for key in primary_keys_and_values.keys():
        query.bindparams(bindparam(key))
    try:
        connection = engine.connect() 
        connection.execute(query, primary_keys_and_values)
    except:
        connection.rollback()
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



def update_to_unify_entries(engine:Engine, table_name:str, attribute_to_change:str, old_values:list, new_value:str, commit:bool):
    query = f'UPDATE {convert_string_if_contains_capitals(table_name, engine.dialect.name)} SET {convert_string_if_contains_capitals(attribute_to_change, engine.dialect.name)} = :new_value'
    cols_and_dtypes = get_column_names_data_types_and_max_length(engine, table_name)
    condition_dict = {}
    print(old_values)
    for index, key in enumerate(cols_and_dtypes.keys()):
        if key == attribute_to_change:
            if cols_and_dtypes[key]['data_type_group']:#[1] == 1:
                new_value = int(new_value)
                for index, item in enumerate(old_values):
                    old_values[index] = int(item)
                break
            elif cols_and_dtypes[key]['data_type_group']:#[1] == 2:
                new_value = float(new_value)
                for item in enumerate(old_values):
                    old_values[index] = float(item)
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
    print('UPDATE-Anweisung:', query)
    # for key in condition_dict.keys():
    #     query.bindparams(bindparam(key))
    # # with engine.connect() as connection:
    #     result = connection.execute(query, condition_dict)
    #     if commit:
    #         connection.commit()
    return execute_sql_query(engine, query, condition_dict, True, commit)

def get_row_number_of_affected_entries(engine:Engine, table_name:str, cols_dtypes_numbertypes_and_length:dict, affected_attributes:list, primary_keys:list, old_values:list, mode:str, convert:bool = True):
    if not mode == 'replace' and not mode == 'unify':
        raise QueryError('Nur die Modi \'replace\' und \'unify\' werden unterstützt.')
    elif mode == 'unify' and len(affected_attributes) != 1:
        raise QueryError('Im Modus \'unify\' kann nur ein Attribut bearbeitet werden.')
    elif mode == 'replace' and len(old_values) != 1:
        raise QueryError('Im Modus \'replace\' kann nur ein Wert ersetzt werden.')
    
    operator, cast_data_type = set_operator_and_cast_data_type(engine.dialect.name)
    key_for_ordering = ', '.join(primary_keys)
    columns_to_select = '*'
    if engine.dialect.name == 'mariadb':
        for key in cols_dtypes_numbertypes_and_length.keys():
            if columns_to_select == '*':
                columns_to_select = key
            else:
                columns_to_select = f'{columns_to_select}, {key}'
    query = f"SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY {key_for_ordering}) AS Nr, {columns_to_select} FROM {table_name}) sub"

    condition = 'WHERE'
    condition_params = {}
        
    if mode == 'replace':
        old_value = old_values[0]
        condition_params['old_value'] = old_value
        for index, attribute in enumerate(affected_attributes):
            attribute_to_search = convert_string_if_contains_capitals(attribute, engine.dialect.name).strip()
            if cols_dtypes_numbertypes_and_length[attribute]['data_type_group'] != 0:
                attribute_to_search = f'CAST ({attribute_to_search} AS {cast_data_type})'
            if index == 0:
                condition = f"{condition} sub.{attribute_to_search} {operator} '%' || :old_value || '%'"
            else:
                condition = f"{condition} OR sub.{attribute_to_search} {operator} '%' || :old_value || '%'"

    elif mode == 'unify':
        affected_attribute = affected_attributes[0]
        for index, value in enumerate(old_values):
            if cols_dtypes_numbertypes_and_length[affected_attribute]['data_type_group'] == 0:
                condition_params['value_' + str(index)] = value
            elif cols_dtypes_numbertypes_and_length[affected_attribute]['data_type_group'] == 1:
                condition_params['value_' + str(index)] = int(value)
            elif cols_dtypes_numbertypes_and_length[affected_attribute]['data_type_group'] == 2:
                condition_params['value_' + str(index)] = float(value)
            
            if index == 0:
                condition = f"{condition} sub.{convert_string_if_contains_capitals(affected_attribute, engine.dialect.name)} = :{'value_' + str(index)}"
            else:
                condition = f"{condition} OR sub.{convert_string_if_contains_capitals(affected_attribute, engine.dialect.name)} = :{'value_' + str(index)}"
    query = text(f'{query} {condition}')
    result = execute_sql_query(engine, query, condition_params)
    if convert:
        return convert_result_to_list_of_lists(result)
    else:
        return result


def get_unique_values_for_attribute(engine:Engine, table_name:str, attribute_to_search:str):
    query = text(f'SELECT DISTINCT {attribute_to_search}, COUNT(*) AS Eintragsanzahl FROM {table_name} GROUP BY {attribute_to_search}')
    return convert_result_to_list_of_lists(execute_sql_query(engine, query))

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


def get_primary_key_from_engine(engine:Engine, table_name:str):
    table_name = convert_string_if_contains_capitals(table_name, engine.dialect.name)
    # with engine.connect() as connection:
    #     if engine.dialect.name == 'postgresql':
    #         result = connection.execute(text(f"SELECT a.attname as column_name FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary"))
    #     elif engine.dialect.name == 'mariadb':
    #         result = connection.execute(text(f"SELECT COLUMN_NAME as column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}' AND COLUMN_KEY = 'PRI'"))
    
    query = str()
    if engine.dialect.name == 'postgresql':
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

def get_primary_key_names_and_data_types_from_engine(engine:Engine, table_name:str):
    # with engine.connect() as connection:
    #     if engine.dialect.name == 'postgresql':
    #         result = connection.execute(text(f"SELECT a.attname as column_name, format_type(a.atttypid, a.atttypmod) AS data_type FROM  pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary"))
    #     # Ergebnisse werden als Liste von Tupeln im Format '[('Primärschlüssel',)]' ausgegeben 
    #     elif engine.dialect.name == 'mariadb':
    #         result = connection.execute(text(f"SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}' AND COLUMN_KEY = 'PRI'"))
    query = str()
    if engine.dialect.name == 'postgresql':
        query = text(f"SELECT a.attname as column_name, format_type(a.atttypid, a.atttypmod) AS data_type FROM  pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary")
    elif engine.dialect.name == 'mariadb':
        query = text(f"SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table_name}' AND COLUMN_KEY = 'PRI'")
    result = execute_sql_query(engine, query)
    
    key_data_type_dict = dict()
    for item in result:
        as_tuple = tuple(item)
        key_data_type_dict[as_tuple[0]] = as_tuple[1]
    return key_data_type_dict

def get_row_count_from_engine(engine, table_name:str):
    # with engine.connect() as connection:
    #     res = connection.execute(text(f'SELECT COUNT(1) FROM {table_name}'))
    # https://datawookie.dev/blog/2021/01/sqlalchemy-efficient-counting/
    query = text(f'SELECT COUNT(1) FROM {table_name}')
    res = execute_sql_query(engine, query)
    return res.fetchone()[0] 

def build_update_query(engine:Engine, table_name:str, column_names_of_affected_attributes:tuple, column_names_of_condition:tuple, values:tuple, operator:str):
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
    condition = build_sql_condition(column_names_of_condition, engine.dialect.name, operator)
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
            condition = f'{condition} {convert_string_if_contains_capitals(item, db_dialect)} = :{item}'
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
    if params != None:
        for key in params.keys():
            query.bindparams(bindparam(key))
    try:
        connection = engine.connect()
        if engine.dialect.name == 'mariadb':
            connection.execute(text("SET sql_mode='POSTGRESQL'"))
        result = connection.execute(query, params)
    except Exception as error:
        if raise_exceptions:
            print(type(error), str(error))
            raise error
        else:
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

def check_data_type_and_constraint_compatibility(engine:Engine, table_name:str, column_name:str, is_text_int_float_or_other:int, input, old_value:str):
    if type(input) not in (str, int, float):
        print('Datentyp kann nicht überprüft werden.')
    table_name = convert_string_if_contains_capitals(table_name, engine.dialect.name)
    column_name = convert_string_if_contains_capitals(column_name, engine.dialect.name)
    update_params = {}
    update_params['old_value'] = old_value
    pre_query = f'SELECT {column_name} FROM {table_name} WHERE'
    operator, cast_data_type = set_operator_and_cast_data_type(engine.dialect.name) 
    if is_text_int_float_or_other == 0:
        pre_query = f"{pre_query} {column_name} {operator} '%' || :old_value || '%' LIMIT 1"
    else:
        pre_query = f"{pre_query} CAST({column_name} AS {cast_data_type}) {operator} '%' || :old_value || '%' LIMIT 1"
    print(pre_query)
    try:
        result = convert_result_to_list_of_lists(execute_sql_query(engine, text(pre_query), update_params, raise_exceptions = True, commit = False))
    except Exception as error:
        raise error
    else:
        print(len(result))
        if len(result) == 0 or result == None:
            return 4
        condition_value = result[0][0]

        query = f'UPDATE {table_name} SET {column_name}'
        condition = f'WHERE {column_name} = :condition_value'
        flag = ''
        if engine.dialect.name == 'postgresql':
            flag = ", 'g'"
        if is_text_int_float_or_other == 0:
            query = f"{query} = regexp_replace({column_name}, :old_value, :new_value{flag})"
        else:
            query = f'{query} = :new_value'
            update_params.pop('old_value')
            
        update_params['new_value'] = input
        update_params['condition_value'] = condition_value
        query = text(f'{query} {condition}')
        
        try:
            execute_sql_query(engine, query, update_params, raise_exceptions = True, commit = False)
        except Exception as error:
            raise error
        return 0
    
        
def set_operator_and_cast_data_type(db_dialect:str):
    operator = ''
    cast_data_type = ''
    if db_dialect == 'mariadb':
        operator = 'LIKE'
        cast_data_type = 'CHAR'
    elif db_dialect == 'postgresql':
        operator = 'ILIKE'
        cast_data_type = 'TEXT'
    return operator, cast_data_type



    
# setzt String in doppelte Anführungszeichen, wenn darin Großbuchstaben enthalten sind (nötig für Tabellen- und Spaltennamen in PostgreSQL)
def convert_string_if_contains_capitals(string:str, db_dialect:str):
    if db_dialect == 'postgresql' and any([x.isupper() for x in string]):
        string = f'"{string}"'
    return string



    
def escape_string(db_dialect:str, string:str):
    if db_dialect == 'postgresql':
        string = string.replace('%', '\%').replace('_', '\_').replace("'", "\'").replace('"', '\"').replace('\\', '\\\\')
    elif db_dialect == 'mariadb':
        string = string.replace('%', '\\%').replace('_', '\\_').replace("'", "\\'").replace('"', '\\"').replace('\\', '\\\\\\\\')
    return string

def convert_result_to_list_of_lists(sql_result:CursorResult):
    result_list = [list(row) for row in sql_result.all()]
    return result_list  



if __name__ == '__main__':
    engine = connect_to_db('postgres', 'arc-en-ciel', 'localhost', 5432, 'Test', 'postgresql', 'utf8')
    cols = get_column_names_data_types_and_max_length(engine, 'studierende')
    ls = get_indexes_of_affected_attributes_for_replacing(engine, 'studierende', cols, ['matrikelnummer'], '2', 'matrikelnummer')
    yo = get_full_table_ordered_by_primary_key(engine, 'studierende', ['matrikelnummer'])
    for line in yo:
        print('line: ', line)
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


