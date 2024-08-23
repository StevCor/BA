from argparse import ArgumentError
from math import ceil
from sqlalchemy import Engine, text
from ControllerClasses import TableMetaData
from model.CompatibilityClasses import MariaInt, PostgresInt, get_int_value_by_dialect_name
from model.databaseModel import get_primary_key_from_engine, convert_result_to_list_of_lists, execute_sql_query, convert_string_if_contains_capitals
from model.SQLDatabaseError import DialectError, QueryError

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
            constraint_query = f"SELECT a.attname, i.table_name FROM pg_constraint con JOIN pg_attribute a ON a.attnum = ANY(con.conkey) JOIN information_schema.columns i ON a.attname = i.column_name AND"
            if len(engines) == 1:
                table_name_1 = convert_string_if_contains_capitals(table_names[0], engine.dialect.name)
                table_name_2 = convert_string_if_contains_capitals(table_names[1], engine.dialect.name)
                constraint_query = f"{constraint_query} (i.table_name = '{table_name_1}' OR i.table_name = '{table_name_2}') WHERE (con.conrelid = ('{table_name_1}'::regclass) OR con.conrelid = ('{table_name_2}'::regclass)) AND con.conrelid = a.attrelid AND (con.contype = 'p' OR con.contype = 'u') AND i.table_catalog = '{engine.url.database}'"
            else:
                table_name = convert_string_if_contains_capitals(table_names[index], engine.dialect.name)
                constraint_query = f"{constraint_query} i.table_name = '{table_name}' WHERE con.conrelid = ('{table_name}'::regclass) AND con.conrelid = a.attrelid AND (con.contype = 'p' OR con.contype = 'u') AND i.table_catalog = '{engine.url.database}'"
            constraint_result = execute_sql_query(engine, text(constraint_query))
            unique_list = []
            for entry in constraint_result:
                if [entry[1], entry[0]] not in unique_list:
                    unique_list.append([entry[1], entry[0]])
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
                if ('int' in data_type or 'bool' in data_type) and numeric_scale == 0:
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
            column_name_1 = convert_string_if_contains_capitals(column_name_1, engine_1)
            column_name_2 = convert_string_if_contains_capitals(column_name_2, engine_2)
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