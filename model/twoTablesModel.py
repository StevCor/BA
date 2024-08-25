from argparse import ArgumentError
from math import ceil
from sqlalchemy import Engine, text
from ControllerClasses import TableMetaData
from model.CompatibilityClasses import MariaInt, PostgresInt, get_int_value_by_dialect_name
from model.databaseModel import get_primary_key_from_engine, convert_result_to_list_of_lists, execute_sql_query, convert_string_if_contains_capitals_or_spaces, list_attributes_to_select
from model.SQLDatabaseError import DialectError, QueryError



def join_tables_of_same_dialect_on_same_server(table_meta_data:list[TableMetaData], attributes_to_join_on:list[dict[str:str]], attributes_to_select_1:list[str], attributes_to_select_2:list[str], full_outer_join:bool = False, cast_direction:int = None):
    engine_1 = table_meta_data[0].engine
    engine_2 = table_meta_data[1].engine
    dialect_1 = engine_1.dialect.name
    dialect_2 = engine_2.dialect.name
    if engine_1.url.host != engine_2.url.host or engine_1.url.port != engine_2.url.port:
        raise ArgumentError(None, 'Mit dieser Funktion können nur Tabellen verbunden werden, die auf demselben Server liegen.')
    elif dialect_1 != dialect_2:
        raise ArgumentError(None, 'Die SQL-Dialekte der verwendeten Engines müssen übereinstimmen.')
    try:
        check_arguments_for_joining(table_meta_data, attributes_to_join_on, attributes_to_select_1, attributes_to_select_2, cast_direction)
    except Exception as error:
        raise error
    
    
    table_1 = f'{convert_string_if_contains_capitals_or_spaces(table_meta_data[0].table, dialect_1)}'
    table_2 = f'{convert_string_if_contains_capitals_or_spaces(table_meta_data[1].table, dialect_2)}'
    db_name_1 = None
    db_name_2 = None
    if table_meta_data[0].engine.url.database != table_meta_data[1].engine.url.database:
        db_name_1 = table_meta_data[0].engine.url.database
        db_name_2 = table_meta_data[1].engine.url.database
    attribute_to_join_on_1 = list(attributes_to_join_on[0].keys())[0]
    attribute_to_join_on_2 = list(attributes_to_join_on[1].keys())[0]
    join_attribute_1 = f'{table_1}.{convert_string_if_contains_capitals_or_spaces(attribute_to_join_on_1, dialect_1)}'
    join_attribute_2 = f'{table_2}.{convert_string_if_contains_capitals_or_spaces(attribute_to_join_on_2, dialect_2)}'
    

    if attribute_to_join_on_1 in attributes_to_select_1 and attribute_to_join_on_2 in attributes_to_select_2:
        attributes_to_select_2.pop(attributes_to_select_2.index(attribute_to_join_on_2))
    delimiter = ','
    if len(attributes_to_select_1) == 0:
        delimiter = ''
        attributes_table_1 = ''
    else:
        attributes_table_1 = list_attributes_to_select(attributes_to_select_1, dialect_1, table_1, db_name_1)
    if len(attributes_to_select_2) == 0:
        delimiter = ''
        
    join_query = f'SELECT {attributes_table_1}{delimiter}'
    if len(attributes_to_select_2) > 0:
        attributes_table_2 = list_attributes_to_select(attributes_to_select_2, dialect_2, table_2, db_name_2)
        join_query = f'{join_query} {attributes_table_2}'
    

    data_type_1 = attributes_to_join_on[0][attribute_to_join_on_1]
    data_type_2 = attributes_to_join_on[1][attribute_to_join_on_2]
    if cast_direction != None:
        if cast_direction == 1:
            join_attribute_1 = f'CAST ({join_attribute_1} AS {data_type_2})'
        elif cast_direction == 2:
            join_attribute_2 = f'CAST ({join_attribute_2} AS {data_type_1})'
    
    if full_outer_join:
        if dialect_1 == 'mariadb':
            join_query = f'{join_query} FROM {table_1} LEFT JOIN {table_2} ON {join_attribute_1} = {join_attribute_2} UNION ({join_query} FROM {table_1} RIGHT JOIN {table_2} ON {join_attribute_1} = {join_attribute_2})'

        
        elif dialect_1 == 'postgresql':
            join_query = f'{join_query} FROM {table_1} FULL OUTER JOIN {table_2} ON {join_attribute_1} = {join_attribute_2}'
            filter_count_table_1 = f'{table_2}.{convert_string_if_contains_capitals_or_spaces(table_meta_data[1].primary_keys[0], dialect_2)}'
            filter_count_table_2 = f'{table_1}.{convert_string_if_contains_capitals_or_spaces(table_meta_data[0].primary_keys[0], dialect_1)}'
            count_query = f'SELECT COUNT(*) FILTER (WHERE {filter_count_table_1} IS NULL), COUNT(*) FILTER (WHERE {filter_count_table_2} IS NULL) FROM {table_1} FULL JOIN {table_2} ON {join_attribute_1} = {join_attribute_2}'
            count_result = execute_sql_query(engine_1, text(count_query))
            counts = list(count_result.fetchone())     
    else:
        if dialect_1 == 'mariadb' or dialect_1 == 'postgresql':
            join_query = f'{join_query} FROM {table_1} INNER JOIN {table_2} ON {join_attribute_1} = {join_attribute_2}'

    joined_table_result = convert_result_to_list_of_lists(execute_sql_query(engine_1, text(join_query)))

    unmatched_rows = {}
    condition_attributes = [attributes_to_select_1, attributes_to_select_2]
    tables = [table_1, table_2] 
    db_names = [db_name_1, db_name_2]
    dialects = [dialect_1, dialect_2]
    join_attributes = [join_attribute_1, join_attribute_2]
    for index, table in enumerate(tables):
        select_all_from_table = list_attributes_to_select(condition_attributes[index], dialects[index], table, db_names[index])
        if index == 0:
            direction = 'LEFT'
        else:
            direction = 'RIGHT'
        unmatched_rows_query = f'SELECT {select_all_from_table} FROM {table_1} {direction} OUTER JOIN {table_2} ON {join_attribute_1} = {join_attribute_2}'
        condition = f'WHERE {join_attributes[(index + 1) % 2]} IS NULL'
        unmatched_rows_query = f'{unmatched_rows_query} {condition}'
        unmatched_rows[tables[index]] = convert_result_to_list_of_lists(execute_sql_query(table_meta_data[index].engine, text(unmatched_rows_query)))

    return joined_table_result, unmatched_rows
    
def join_tables_of_different_dialects(table_meta_data:list[TableMetaData], attributes_to_join_on:list[dict[str:str]], attributes_to_select_1:list[str], attributes_to_select_2:list[str], full_outer_join:bool = False, cast_direction:int = None):
    try:
        check_arguments_for_joining(table_meta_data, attributes_to_join_on, attributes_to_select_1, attributes_to_select_2, cast_direction)
    except Exception as error:
        raise error
    table_meta_data_1 = table_meta_data[0]
    table_meta_data_2 = table_meta_data[1]
    table_1 = table_meta_data_1.table
    table_2 = table_meta_data_2.table
    tables = [table_1, table_2]
    engines = [table_meta_data_1.engine, table_meta_data_2.engine]
    attributes_to_select = [attributes_to_select_1, attributes_to_select_2]
    results = {}
    result_columns = []
    join_attributes = []
    for index, engine in enumerate(engines):
        selection = ', '.join(attributes_to_select[index])
        join_attributes.append(list(attributes_to_join_on[index].keys())[0])
        if join_attributes[index] not in attributes_to_select[index]:
            selection = f'{selection}, {join_attributes[index]}'
        query = f'SELECT {selection} FROM {convert_string_if_contains_capitals_or_spaces(tables[index], engine.dialect.name)}'
        result = execute_sql_query(engine, text(query))
        result_columns.append(list(result.keys()))
        results[tables[index]] = convert_result_to_list_of_lists(result)
       
    data_type_1 = attributes_to_join_on[0][join_attributes[0]] 
    data_type_2 = attributes_to_join_on[1][join_attributes[1]] 
    join_attribute_index_1 = result_columns[0].index(join_attributes[0])
    join_attribute_index_2 = result_columns[1].index(join_attributes[1])
    joined_table = []
    match_counter_table_2 = [0] * len(results[table_2])
    for row_1 in results[table_1]:
        print('row_1: ', row_1)
        row_1_match_counter = 0
        for row_index, row_2 in enumerate(results[table_2]):
            print('row_2: ', row_2)
            is_match = False
            if row_1[join_attribute_index_1] != None and row_2[join_attribute_index_2] != None:
                if row_1[join_attribute_index_1] == row_2[join_attribute_index_2]:
                    is_match = True
                elif data_type_1 == 'boolean':
                    if data_type_2 == 'integer':
                        if int(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                            is_match = True
                    elif data_type_2 == 'decimal':
                        if float(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                            is_match = True
                    elif data_type_2 == 'text':
                        if str(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                            is_match = True
                elif data_type_1 == 'integer':
                    if data_type_2 == 'boolean':
                        if row_1[join_attribute_index_1] == int(row_2[join_attribute_index_2]):
                            is_match = True
                    elif data_type_2 == 'text':
                        if str(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                            is_match = True
                elif data_type_1 == 'decimal':
                    if data_type_2 == 'text':
                        if str(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                            is_match = True
                elif data_type_1 == 'text':
                    if row_1[join_attribute_index_1] == str(row_2[join_attribute_index_2]):
                        is_match = True
            row_2_copy = row_2.copy()
            if join_attributes[1] in attributes_to_select_2:
                row_2_copy.pop(join_attribute_index_2)
            if is_match:
                row_1_match_counter += 1
                match_counter_table_2[row_index] += 1
                joined_table.append(row_1.copy()+ row_2_copy)
            else:
                if full_outer_join and row_index == len(results[table_2]) - 1:
                    if match_counter_table_2[row_index] == 0:
                        joined_table.append(([None] * len(results[table_1][0]) + row_2_copy))
        if full_outer_join and row_1_match_counter == 0:
            if join_attributes[1] in attributes_to_select_2:
                empty_values = len(attributes_to_select_2) - 1
            else:
                empty_values = len(attributes_to_select_2)
            joined_table.append(row_1 + [None] * empty_values)
    if join_attributes[1]  in attributes_to_select_2:
        result_columns[1].pop(join_attribute_index_2)
    column_names_for_display = [table_1 + '.' + col_1 for col_1 in result_columns[0]] + [table_2 + '.' + col_2 for col_2 in result_columns[1]]
    return joined_table, column_names_for_display




def check_arguments_for_joining(table_meta_data:list[TableMetaData], attributes_to_join_on:list[dict[str:str]], attributes_to_select_1:list[str], attributes_to_select_2:list[str], cast_direction:int = None):
    engine_1 = table_meta_data[0].engine
    engine_2 = table_meta_data[1].engine
    dialect_1 = engine_1.dialect.name
    dialect_2 = engine_2.dialect.name
    if dialect_1 not in ('mariadb', 'postgresql') or dialect_2 not in ('mariadb', 'postgresql'):
        raise DialectError(f'Ein angegebener SQL-Dialekt wird nicht unterstützt.')
    elif len(attributes_to_join_on) != 2:
        raise ArgumentError(None, 'Die Tabellen können nur über zwei Attribute miteinander verbunden werden.')
    elif len(table_meta_data) != 2:
        raise ArgumentError(None, 'Es müssen exakt zwei Tabellen angegeben werden, die verbunden werden sollen.')
    elif len(attributes_to_select_1) + len(attributes_to_select_2) <= 0:
        raise ArgumentError(None, 'Es muss mindestens ein Attribut ausgewählt werden, das zurückgegeben werden soll.')
    elif cast_direction not in (None, 1, 2):
        raise ArgumentError(None, 'Bitte geben Sie den Wert 1 an, wenn das Verbindungsattribut von Tabelle 1 konvertiert werden soll, 2 für eine Konversion des Verbindungsattributs von Tabelle 2 und für das Auslassen von Konversionen den Wert None.')
    elif any([type(item) != TableMetaData for item in table_meta_data]):
        raise TypeError(None, 'Die Tabellenmetadaten müssen vom Typ TableMetaData sein.')



### ZU viel mit Kompatibilität aufgehalten

def check_data_type_meta_data(engines:list[Engine], table_names:list[str]):
    if not 0 < len(engines) <= 2 or len(table_names) != 2:
        raise ArgumentError('Es können nur eine Engine mit zwei Tabellen oder zwei Engines mit jeweils einer Tabelle überprüft werden.')
    result_dict = {}
    result_dict['table_1'] = {}
    result_dict['table_2'] = {}
    for index, engine in enumerate(engines):
        query = 'SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION'
        if engine.dialect.name == 'mariadb':
            query = f'{query}, COLUMN_KEY, COLUMN_TYPE FROM information_schema.columns WHERE TABLE_SCHEMA = DATABASE()'
        elif engine.dialect.name == 'postgresql':
            query = f"{query} FROM information_schema.columns WHERE TABLE_CATALOG = '{engine.url.database}'"
        else:
            raise QueryError('Dieser SQL-Dialekt wird nicht unterstüzt.')
        if len(engines) == 1:
            query = f"{query} AND (TABLE_NAME = '{table_names[0]}' OR TABLE_NAME = '{table_names[1]}')"
        else:
            query = f"{query} AND TABLE_NAME = '{table_names[index]}'"
        result = convert_result_to_list_of_lists(execute_sql_query(engines[index], text(query)))
        print(query)
        # für PostgreSQL fehlt noch die Angabe, ob es sich um einen Primärschlüssel oder ein Attribut mit Unique-Constraint handelt
        if engine.dialect.name == 'postgresql':
            constraint_query = f"SELECT i.table_name, a.attnameFROM pg_constraint con JOIN pg_attribute a ON a.attnum = ANY(con.conkey) JOIN information_schema.columns i ON a.attname = i.column_name AND"
            if len(engines) == 1:
                table_name_1 = convert_string_if_contains_capitals_or_spaces(table_names[0], engine.dialect.name)
                table_name_2 = convert_string_if_contains_capitals_or_spaces(table_names[1], engine.dialect.name)
                constraint_query = f"{constraint_query} (i.table_name = '{table_name_1}' OR i.table_name = '{table_name_2}') WHERE (con.conrelid = ('{table_name_1}'::regclass) OR con.conrelid = ('{table_name_2}'::regclass)) AND con.conrelid = a.attrelid AND (con.contype = 'p' OR con.contype = 'u') AND i.table_catalog = '{engine.url.database}'"
            else:
                table_name = convert_string_if_contains_capitals_or_spaces(table_names[index], engine.dialect.name)
                constraint_query = f"{constraint_query} i.table_name = '{table_name}' WHERE con.conrelid = ('{table_name}'::regclass) AND con.conrelid = a.attrelid AND (con.contype = 'p' OR con.contype = 'u') AND i.table_catalog = '{engine.url.database}'"
            constraint_result = execute_sql_query(engine, text(constraint_query))
            unique_list = []
            for entry in constraint_result:
                if [entry[0], entry[1]] not in unique_list:
                    unique_list.append([entry[0], entry[1]])
        for row in result:
            table_name = row[0]
            if table_name == table_names[0]:
                table_key = 'table_1'
            else:
                table_key = 'table_2'
            column_name = row[1]
            if row[2] == 'YES':
                is_nullable = True
            else:
                is_nullable = False                
            data_type = row[3]
            character_max_length = row[4]
            numeric_precision = row[5]
            numeric_scale = row[6]
            datetime_precision = row[7]
            if (engine.dialect.name == 'mariadb' and (row[8] == 'PRI' or row[8] == 'UNI')) or (engine.dialect.name == 'postgresql' and [table_name, column_name] in unique_list):
                is_unique = True
            else:
                is_unique = False  
            if (engine.dialect.name == 'mariadb' and 'unsigned' in row[9].lower()) or (engine.dialect.name == 'postgresql' and (data_type == 'boolean' or 'serial' in data_type)):
                is_unsigned = True
            else:
                is_unsigned = None
            result_dict[table_key][column_name] = {}
            if datetime_precision != None:
                result_dict[table_key][column_name] = {'data_type_group': 'date', 'data_type': data_type, 'datetime_precision': datetime_precision}
            elif character_max_length != None:
                result_dict[table_key][column_name] = {'data_type_group': 'text', 'data_type': data_type, 'character_max_length': character_max_length}
            elif numeric_precision != None:
                if (engine.dialect.name == 'mariadb' and row[9].lower() == 'tinyint(1)') or (engine.dialect.name == 'postgresql' and data_type == 'boolean'):
                    result_dict[table_key][column_name] = {'data_type_group': 'boolean', 'data_type': 'boolean'}
                elif 'int' in data_type or numeric_scale == 0:
                    result_dict[table_key][column_name] = {'data_type_group': 'integer', 'data_type': data_type, 'numeric_precision': numeric_precision}
                else:
                    result_dict[table_key][column_name] = {'data_type_group': 'decimal', 'data_type': data_type, 'numeric_precision': numeric_precision, 'numeric_scale': numeric_scale}
            else: 
                result_dict[table_key][column_name] = {'data_type_group': data_type, 'data_type': data_type}
            if is_unsigned != None:
                result_dict[table_key][column_name]['unsigned'] = is_unsigned
            result_dict[table_key][column_name]['is_nullable'] = is_nullable
            result_dict[table_key][column_name]['is_unique'] = is_unique
    return result_dict

def check_basic_data_type_compatibility(table_meta_data_1:TableMetaData, table_meta_data_2:TableMetaData, table_1_is_target:bool):
    engine_1 = table_meta_data_1.engine
    engine_2 = table_meta_data_2.engine
    if  engine_1 == engine_2:
        engines = [engine_1]
    else:
        engines = [engine_1, engine_2]
    table_1 = table_meta_data_1.table
    table_2 = table_meta_data_2.table
    table_names = [table_1, table_2]
    dtype_info = check_data_type_meta_data(engines, table_names)
    print(dtype_info)
    table_key_1 = 'table_1'
    table_key_2 = 'table_2'
    dict_table_1 = dtype_info[table_key_1]
    dict_table_2 = dtype_info[table_key_2]
    print(dict_table_1)
    print(dict_table_2)
    compatibility_matrix = []
    compatibility_by_code = {}
    for column_name_1 in table_meta_data_1.columns:
        row_list = []
        for column_name_2 in table_meta_data_2.columns:
            dgroup_1 = dict_table_1[column_name_1]['data_type_group']
            dgroup_2 = dict_table_2[column_name_2]['data_type_group']
            dtype_1 = dict_table_1[column_name_1]['data_type'].lower()
            dtype_2 = dict_table_2[column_name_2]['data_type'].lower()
            # Code für Kompatibilität. 0 bei fehlender Kompatibilität; 1 bei voller Kompatibilität; 2 bei ggf. uneindeutigen Einträgen des Attributs;
            # 3, wenn ggf. Typkonversionen nötig sind; 4, wenn definitiv Typkonversionen notwendig sind. Durch Kombination können sich zudem die Werte
            # 5 für ggf. nicht eindeutige Werte mit ggf. nötigen Typkonversionen und 6 für ggf. nicht eindeutige Werte mit nötigen Typkonversionen ergeben.
            comp_code = 0
            if (dict_table_1[column_name_1] == dict_table_2[column_name_2]) or ('bool' in dtype_1 and 'bool' in dtype_2) or ('int' in dtype_1 and 'int' in dtype_2) or ('serial' in dtype_1 and 'serial' in dtype_2):
                comp_code = 1
            else:
                if not dict_table_1[column_name_1]['is_unique'] or not dict_table_2[column_name_2]['is_unique']:
                    comp_code = 2
                if (dgroup_1 == dgroup_2) or (dgroup_1 in ('integer', 'decimal') and dgroup_2 in ('integer', 'decimal')):
                    comp_code += 3
                elif dgroup_1 in ('integer', 'decimal', 'text') and dgroup_2 in ('integer', 'decimal', 'text'):
                    comp_code += 4
            row_list.append(comp_code)
            if comp_code not in compatibility_by_code.keys():
                compatibility_by_code[comp_code] = [(column_name_1, column_name_2)]
            else:
                compatibility_by_code[comp_code].append((column_name_1, column_name_2))
        compatibility_matrix.append(row_list)

    return compatibility_matrix, compatibility_by_code, dtype_info


def check_data_type_compatibility(table_meta_data_1:TableMetaData, table_meta_data_2:TableMetaData, table_1_is_target:bool):
    engine_1 = table_meta_data_1.engine
    engine_2 = table_meta_data_2.engine
    if  engine_1 == engine_2:
        engines = [engine_1]
    else:
        engines = [engine_1, engine_2]
    table_1 = table_meta_data_1.table
    table_2 = table_meta_data_2.table
    table_names = [table_1, table_2]
    dtype_info = check_data_type_meta_data(engines, table_names)
    table_key_1 = 'table_1'
    table_key_2 = 'table_2'
    cast_matrix = []
    matrix_list = []
    for column_name_1 in table_meta_data_1.columns:
        cast_list = []
        row_list = []
        for column_name_2 in table_meta_data_2.columns:
            column_name_1 = convert_string_if_contains_capitals_or_spaces(column_name_1, engine_1)
            column_name_2 = convert_string_if_contains_capitals_or_spaces(column_name_2, engine_2)
            if engine_1.dialect.name == engine_2.dialect.name:
                # wenn alle Einträge im Datentypinfo-Dictionary gleich sind, sind die Datentypen vollständig identisch und somit kompatibel
                if dtype_info[table_key_1][column_name_1] == dtype_info[table_key_1][column_name_1]:
                    cast_list.append(1)
                    row_list.append(1)
                elif dtype_info[table_key_1][column_name_1]['data_type'] == dtype_info[table_key_2][column_name_2]['data_type']:
                    data_type_1 = dtype_info[table_key_1][column_name_1]['data_type']
                    data_type_2 = dtype_info[table_key_2][column_name_2]['data_type']
                    if dtype_info[table_key_1][column_name_1]['data_type_group'] == 'date':
                        precision_1 = dtype_info[table_key_1][column_name_1]['datetime_precision']
                        precision_2 = dtype_info[table_key_2][column_name_2]['datetime_precision']
                        if precision_1 < precision_2:
                            cast_list.append(f'CAST ({column_name_1} AS {data_type_1}({precision_2}))')
                            row_list.append(2)
                        else:
                            cast_list.append(f'CAST ({column_name_2} AS {data_type_2}({precision_1}))')
                            row_list.append(3)
                    elif dtype_info[table_key_1][column_name_1]['data_type_group'] == 'text':
                        max_length_1 = dtype_info[table_key_1][column_name_1]['character_max_length']
                        max_length_2 = dtype_info[table_key_2][column_name_2]['character_max_length']
                        if max_length_1 < max_length_2:
                            cast_list.append(f'CAST ({column_name_1} AS {data_type_2}({max_length_2}))')
                            row_list.append(2)
                        else:
                            cast_list.append(f'CAST ({column_name_2} AS {data_type_1}({max_length_1}))')
                            row_list.append(3)
                    elif dtype_info[table_key_1][column_name_1]['data_type_group'] == 'decimal':
                        precision_1 = dtype_info[table_key_1][column_name_1]['numeric_precision']
                        precision_2 = dtype_info[table_key_2][column_name_2]['numeric_precision']
                        scale_1 = dtype_info[table_key_1][column_name_1]['numeric_scale']
                        scale_2 = dtype_info[table_key_2][column_name_2]['numeric_scale']
                        if precision_1 < precision_2 and scale_1 < scale_2:
                            cast_list.append(f'CAST ({column_name_1} AS {data_type_2}({scale_2}, {precision_2}))')

                elif dtype_info[table_key_1][column_name_1]['data_type_group'] == dtype_info[table_key_1][column_name_1]['data_type_group']:
                    pass
    return 0

def get_integer_type_to_cast_to(engine_dialect_1:str, dtype_dict_1:dict, engine_dialect_2:str, dtype_dict_2:dict):
    if engine_dialect_1 not in ('mariadb', 'postgresql') or engine_dialect_2 not in ('mariadb', 'postgresql'):
        raise DialectError('Einer der angegebenen SQL-Dialekte wird nicht unterstützt.')
    column_name_1 = dtype_dict_1.keys[0]
    column_name_2 = dtype_dict_2.keys[0]
    dgroup_1 = dtype_dict_1[column_name_1]['data_type_group']
    dgroup_2 = dtype_dict_2[column_name_2]['data_type_group']
    if (dgroup_1 != 'integer' and dgroup_2 != 'integer') or (dgroup_1 != 'decimal' or dgroup_1 != 'decimal'):
        raise ArgumentError('Diese Überprüfung erfolgt nur für zwei Datentypen, von denen einer eine Dezimalzahl und der andere eine ganze Zahl ist.')
    dtype_1 = dtype_dict_1[column_name_1]['data_type']
    dtype_2 = dtype_dict_2[column_name_2]['data_type']
    size = 0
    numeric_precision = 0
    numeric_scale = 0
    table_no_of_decimal_type = 0
    column_name = None
    cast_list = None
    if dgroup_1 == 'integer':
        size = get_int_value_by_dialect_name(engine_dialect_1, dtype_1)
        table_no_of_decimal_type = 2
        numeric_precision = int(dtype_dict_2[column_name_2]['numeric_precision']) 
        numeric_scale = int(dtype_dict_2[column_name_2]['numeric_scale'])
        column_name = column_name_2
    else:
        size = get_int_value_by_dialect_name(engine_dialect_2, dtype_2)
        table_no_of_decimal_type = 1
        numeric_precision = int(dtype_dict_1[column_name_1]['numeric_precision']) 
        numeric_scale = int(dtype_dict_1[column_name_1]['numeric_scale'])
        column_name = column_name_1
    integer_digits = numeric_precision - numeric_scale
    if integer_digits == numeric_precision:
        pass
    else:
        cast_list = [None, None]
        enum_value = 0
        if engine_dialect_1 == 'mariadb':
            if engine_dialect_2 == 'mariadb':
                if integer_digits > size:
                    add_to_value = int(ceil(integer_digits - size)) * 8
                    enum_value = size + add_to_value
                else:
                    enum_value = size
                cast_list[table_no_of_decimal_type-1] = f'CAST ({column_name} AS {MariaInt(size).name})'
            elif engine_dialect_2 == 'postgresql':
                pass

    return cast_list


def check_integer_compatibility(engine_1:Engine, dtype_dict_1:dict, engine_2:Engine, dtype_dict_2:dict, cast:bool = False):
    for key in ['data_type_group', 'data_type', 'numeric_precision']:
        if key not in dtype_dict_1.keys() or key not in dtype_dict_1.keys():
            raise ValueError('Eines der übergebenen Dictionarys enthält nicht die erforderlichen Werte.')
    dialect_1 = engine_1.dialect.name
    dialect_2 = engine_2.dialect.name
    dtype_1 = dtype_dict_1['data_type']
    dtype_2 = dtype_dict_2['data_type']
    precision_1 = dtype_dict_1['numeric_precision']
    precision_2 = dtype_dict_2['numeric_precision']
    size_1 = None
    size_2 = None
    # Kürzel für Unterschied zwischen den Dialekten: 'mm', wenn beides MariaDB ist; 'pp', wenn beides PostgreSQL ist; 'mp', wenn der erste
    # Dialekt MariaDB und der zweite PostgreSQL ist; 'pm', wenn der erste Dialekt PostgreSQL und der zweite MariaDB ist.
    dialect_flag = ''
    if dialect_1 == 'mariadb':
        size_1 = MariaInt.value_of(dtype_1)
        if dialect_2 == 'mariadb':
            size_2 = MariaInt.value_of(dtype_2)
            dialect_flag = 'mm'
        elif dialect_2 == 'postgresql':
            size_2 = PostgresInt.value_of(dtype_2)
            dialect_flag = 'mp'
        else: 
            raise DialectError(f'Der Dialekt {dialect_2} wird nicht unterstützt.')
    elif dialect_1 == 'postgresql':
        size_1 = PostgresInt.value_of(dtype_1)
        if dialect_2 == 'mariadb':
            size_2 = MariaInt.value_of(dtype_2)
            dialect_flag = 'pm'
        elif dialect_2 == 'postgresql':
            size_2 = PostgresInt.value_of(dtype_2)
            dialect_flag = 'pp'
        else: 
            raise DialectError(f'Der Dialekt {dialect_2} wird nicht unterstützt.')
    else:
        raise DialectError(f'Der Dialekt {dialect_1} wird nicht unterstützt.')
    if 'is_unsigned' in dtype_dict_1.keys():
        size_1 += 1
    if 'is_unsigned' in dtype_dict_2.keys():
        size_2 += 1
    if dialect_flag == 'mm':
        if size_1 == size_2:
            return False
    elif dialect_flag == 'mp':
        return 'wtf'
    
def check_if_data_types_require_casting(engine_1:Engine, dtype_dict_1:dict, engine_2:Engine, dtype_dict_2:dict):
    dialect_1 = engine_1.dialect.name
    dialect_2 = engine_2.dialect.name
    dtype_1 = dtype_dict_1['data_type']
    dtype_2 = dtype_dict_2['data_type']
    dgroup_1 = dtype_dict_1['data_type_group']
    dgroup_2 = dtype_dict_2['data_type_group']
    if (dgroup_1 == 'integer' and dgroup_1 == 'integer') or ('bool' in dtype_1 and 'bool' in dtype_2) or ('int' in dtype_1 and 'int' in dtype_2) or ('serial' in dtype_1 and 'serial' in dtype_2):
        return []
    elif dgroup_1 == 'integer' and dgroup_2 == 'decimal':
        pass
        



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