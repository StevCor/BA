from argparse import ArgumentError
from dateutil import parser
from math import ceil
from sqlalchemy import Engine, text
from ControllerClasses import TableMetaData
from model.CompatibilityClasses import MariaInt, PostgresInt, get_int_value_by_dialect_name, MariaToPostgresCompatibility, PostgresToMariaCompatibility
from model.databaseModel import get_primary_key_from_engine, convert_result_to_list_of_lists, execute_sql_query, convert_string_if_contains_capitals_or_spaces
from model.SQLDatabaseError import DialectError, MergeError, QueryError



def join_tables_of_same_dialect_on_same_server(table_meta_data:list[TableMetaData], attributes_to_join_on:list[str], attributes_to_select_1:list[str], attributes_to_select_2:list[str], cast_direction:int = 0, full_outer_join:bool = False):
    engine_1 = table_meta_data[0].engine
    engine_2 = table_meta_data[1].engine
    dialect_1 = engine_1.dialect.name
    dialect_2 = engine_2.dialect.name
    if engine_1.url.host != engine_2.url.host or engine_1.url.port != engine_2.url.port:
        raise MergeError('Mit dieser Funktion können nur Tabellen verbunden werden, die auf demselben Server liegen.')
    elif dialect_1 != dialect_2:
        raise ArgumentError(None, 'Die SQL-Dialekte der verwendeten Engines müssen übereinstimmen.')
    elif engine_1.url.database != engine_2.url.database and dialect_1 == 'postgresql':
        raise MergeError('Für den Dialekt PostgreSQL kann diese Funktion nur auf Tabellen der gleichen Datenbank angewendet werden.')
    try:
        check_arguments_for_joining(table_meta_data, attributes_to_join_on, attributes_to_select_1, attributes_to_select_2, cast_direction)
    except Exception as error:
        raise error
    
    
    table_1 = f'{convert_string_if_contains_capitals_or_spaces(table_meta_data[0].table_name, dialect_1)}'
    table_2 = f'{convert_string_if_contains_capitals_or_spaces(table_meta_data[1].table_name, dialect_2)}'
    db_name_1 = None
    db_name_2 = None
    if table_meta_data[0].engine.url.database != table_meta_data[1].engine.url.database:
        db_name_1 = table_meta_data[0].engine.url.database
        db_name_2 = table_meta_data[1].engine.url.database
    join_attribute_1 = f'{table_1}.{convert_string_if_contains_capitals_or_spaces(attributes_to_join_on[0], dialect_1)}'
    join_attribute_2 = f'{table_2}.{convert_string_if_contains_capitals_or_spaces(attributes_to_join_on[1], dialect_2)}'
    

    if attributes_to_join_on[0] in attributes_to_select_1 and attributes_to_join_on[1] in attributes_to_select_2:
        attributes_to_select_2.pop(attributes_to_select_2.index(attributes_to_join_on[1]))
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
    
    data_type_group_1 = table_meta_data[0].get_data_type_group(attributes_to_join_on[0])
    data_type_group_2 = table_meta_data[1].get_data_type_group(attributes_to_join_on[1])
    data_type_1 = table_meta_data[0].get_data_type(attributes_to_join_on[0])
    data_type_2 = table_meta_data[0].get_data_type(attributes_to_join_on[1])

    if cast_direction == 0:
        if data_type_group_1 == 'boolean':
            if data_type_group_2 == 'integer' or data_type_group_2 == 'decimal' or data_type_group_2 == 'text':
                cast_direction = 1
        elif data_type_group_1 == 'integer':
            if data_type_group_2 == 'boolean':
                cast_direction = 2
            elif data_type_group_2 == 'text':
                cast_direction = 1
        elif data_type_group_1 == 'decimal':
            if data_type_group_2 == 'text':
                cast_direction = 1
        elif data_type_group_1 == 'text':
            cast_direction = 2
        elif data_type_group_1 == 'date':
            if data_type_group_2 == 'text':
                cast_direction = 1
            
    if cast_direction != 0:
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
    column_names_for_display = [table_1 + '.' + col_1 for col_1 in attributes_to_select_1] + [table_2 + '.' + col_2 for col_2 in attributes_to_select_2]

    no_of_unmatched_rows = {table_1: 0, table_2: 0}
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
        no_of_unmatched_rows[tables[index]] = len(convert_result_to_list_of_lists(execute_sql_query(table_meta_data[index].engine, text(unmatched_rows_query))))

    return joined_table_result, column_names_for_display, no_of_unmatched_rows, cast_direction
    
def join_tables_of_different_dialects_dbs_or_servers(table_meta_data:list[TableMetaData], attributes_to_join_on:list[str], attributes_to_select_1:list[str], attributes_to_select_2:list[str], cast_direction:int = None, full_outer_join:bool = False):
    try:
        check_arguments_for_joining(table_meta_data, attributes_to_join_on, attributes_to_select_1, attributes_to_select_2, cast_direction)
    except Exception as error:
        raise error
    table_meta_data_1 = table_meta_data[0]
    table_meta_data_2 = table_meta_data[1]
    table_1 = table_meta_data_1.table_name
    table_2 = table_meta_data_2.table_name
    tables = [table_1, table_2]
    engines = [table_meta_data_1.engine, table_meta_data_2.engine]
    attributes_to_select = [attributes_to_select_1, attributes_to_select_2]
    results = {}
    result_columns = []
    join_attributes = []
    for index, engine in enumerate(engines):
        selection = ', '.join(attributes_to_select[index])
        join_attributes.append(attributes_to_join_on[index])
        if join_attributes[index] not in attributes_to_select[index]:
            selection = f'{selection}, {join_attributes[index]}'
        query = f'SELECT {selection} FROM {convert_string_if_contains_capitals_or_spaces(tables[index], engine.dialect.name)}'
        result = execute_sql_query(engine, text(query))
        result_columns.append(list(result.keys()))
        results[tables[index]] = convert_result_to_list_of_lists(result)

    data_type_group_1 = table_meta_data_1.get_data_type_group(attributes_to_join_on[0])
    data_type_group_2 = table_meta_data_2.get_data_type_group(attributes_to_join_on[1])
    join_attribute_index_1 = result_columns[0].index(join_attributes[0])
    join_attribute_index_2 = result_columns[1].index(join_attributes[1])
    joined_table = []
    match_counter_table_2 = [0] * len(results[table_2])
    no_of_unmatched_rows = {table_1: 0, table_2: 0}
    for row_1 in results[table_1]:
        print('row_1: ', row_1)
        row_1_match_counter = 0
        for row_index, row_2 in enumerate(results[table_2]):
            print('row_2: ', row_2)
            is_match = False
            if row_1[join_attribute_index_1] != None and row_2[join_attribute_index_2] != None:
                if row_1[join_attribute_index_1] == row_2[join_attribute_index_2]:
                    is_match = True
                elif cast_direction == 1 or cast_direction == 2:
                    cast_result = force_cast_and_match(data_type_group_1, data_type_group_2, [row_1[join_attribute_index_1], row_2[join_attribute_index_2]], cast_direction)
                    if type(cast_result == tuple) and len(cast_result) == 2:
                        is_match = cast_result[0]
                        if cast_direction == 1:
                            row_1[join_attribute_index_1] = cast_result[1]
                        elif cast_direction == 2:
                            row_2[join_attribute_index_2] = cast_result[1]
                    else:
                        is_match = False
                elif data_type_group_1 == 'boolean':
                    if cast_direction == 0 and data_type_group_2 == 'integer':
                        try:
                            if int(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                                is_match = True
                                row_1[join_attribute_index_1] = int(row_1[join_attribute_index_1])
                        except ValueError:
                            is_match = False
                    elif data_type_group_2 == 'decimal':
                        try:
                            if float(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                                is_match = True
                                row_1[join_attribute_index_1] = float(row_1[join_attribute_index_1])
                        except ValueError:
                            is_match = False
                    elif data_type_group_2 == 'text':
                        try:
                            if str(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                                is_match = True
                                row_1[join_attribute_index_1] = str(row_1[join_attribute_index_1])
                        except ValueError:
                            is_match = False
                elif data_type_group_1 == 'integer':
                    if data_type_group_2 == 'boolean':
                        try:
                            if row_1[join_attribute_index_1] == int(row_2[join_attribute_index_2]):
                                is_match = True
                                row_2[join_attribute_index_2] = int(row_2[join_attribute_index_2])
                        except ValueError:
                            is_match = False
                    elif data_type_group_2 == 'text':
                        try:
                            if str(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                                is_match = True
                                row_1[join_attribute_index_1] = str(row_1[join_attribute_index_1])
                        except ValueError:
                            is_match = False
                elif data_type_group_1 == 'decimal':
                    if data_type_group_2 == 'text':
                        try:
                            if str(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                                is_match = True
                                row_1[join_attribute_index_1] = str(row_1[join_attribute_index_1])
                        except ValueError:
                            is_match = False
                elif data_type_group_1 == 'text':
                    try:
                        if row_1[join_attribute_index_1] == str(row_2[join_attribute_index_2]):
                            is_match = True
                            row_2[join_attribute_index_2] = str(row_2[join_attribute_index_2])
                    except ValueError:
                        is_match = False
                elif data_type_group_1 == 'date':
                    if data_type_group_2 == 'text':
                        try:
                            if str(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                                is_match = True
                                row_1[join_attribute_index_1] = str(row_1[join_attribute_index_1])
                        except ValueError:
                            is_match = False
            row_2_copy = row_2.copy()
            if join_attributes[1] in attributes_to_select_2:
                row_2_copy.pop(join_attribute_index_2)
            if is_match:
                row_1_match_counter += 1
                match_counter_table_2[row_index] += 1
                joined_table.append(row_1.copy()+ row_2_copy)
            else:
                if row_index == len(results[table_2]) - 1 and match_counter_table_2[row_index] == 0:
                    if full_outer_join:
                        joined_table.append(([None] * len(results[table_1][0]) + row_2_copy))       
        if row_1_match_counter == 0:
            if full_outer_join:
                if join_attributes[1] in attributes_to_select_2:
                    empty_values = len(attributes_to_select_2) - 1
                else:
                    empty_values = len(attributes_to_select_2)
                joined_table.append(row_1 + [None] * empty_values)
            no_of_unmatched_rows[table_1] += 1
    for counter in match_counter_table_2:
        if counter == 0:
            no_of_unmatched_rows[table_2] += 1
    if join_attributes[1]  in attributes_to_select_2:
        result_columns[1].pop(join_attribute_index_2)
    column_names_for_display = [table_1 + '.' + col_1 for col_1 in result_columns[0]] + [table_2 + '.' + col_2 for col_2 in result_columns[1]]
    return joined_table, column_names_for_display, no_of_unmatched_rows

def force_cast_and_match(data_type_group_1:str, data_type_group_2:str, values_to_match:list, cast_direction:int):
    if cast_direction not in (1, 2):
        raise ArgumentError('Der Parameter cast_direction darf nur die Werte 1 oder 2 annehmen.')
    elif data_type_group_1 not in ['boolean', 'integer', 'decimal', 'text', 'date'] or data_type_group_2 not in ['boolean', 'integer', 'decimal', 'text', 'date']:
        raise ArgumentError('Mit dieser Funktion können nur Werte überprüft werden, die den Datentypgruppen boolean, integer, decimal, text oder date angehören.')
    value_1 = values_to_match[0]
    value_2 = values_to_match[1]
    if cast_direction == 1:
        if data_type_group_2 == 'integer':
            try:
                value_1 = int(value_1)
            except ValueError:
                return False
        elif data_type_group_2 == 'boolean':
            try:
                value_1 = bool(value_1)
            except ValueError:
                return False
        elif data_type_group_2 == 'decimal':
            try:
                value_1 = float(value_1)
            except ValueError:
                return False
        elif data_type_group_2 == 'text':
            try:
                value_1 = str(value_1)
            except ValueError:
                return False
        elif data_type_group_2 == 'date':
            try:
                value_1 = parser.parse(value_1)
            except (ValueError, TypeError, parser.ParserError):
                return False
        if value_1 == value_2:
            return True, value_1
    elif cast_direction == 2:
        if data_type_group_1 == 'integer':
            try:
                value_2 = int(value_2)
            except ValueError:
                return False
        elif data_type_group_1 == 'boolean':
            try:
                value_2 = bool(value_2)
            except ValueError:
                return False
        elif data_type_group_1 == 'decimal':
            try:
                value_2 = float(value_2)
            except ValueError:
                return False
        elif data_type_group_2 == 'text':
            try:
                value_2 = str(value_2)
            except ValueError:
                return False
        elif data_type_group_2 == 'date':
            try:
                value_2 = parser.parse(value_2)
            except (ValueError, TypeError, parser.ParserError):
                return False
        if value_1 == value_2:
            return True, value_2
    return False


def merge_two_tables(target_table_data:TableMetaData, source_table_data:TableMetaData, attributes_to_join_on:list[str], source_column_to_insert:str, target_column:str = None, cast_direction:int = 0, new_column_name:str = None, commit:bool = False):
    if type(target_table_data) != TableMetaData or type(source_table_data) != TableMetaData:
        raise ArgumentError(None, 'Die Parameter target_table_data und source_table_data müssen vom Typ TableMetaData sein.')
    if source_column_to_insert not in source_table_data.columns:
        raise ArgumentError(None, 'Die zu übernehmende Spalte muss zur Quelltabelle gehören.')
    if target_column is not None and target_column not in target_table_data.columns:
        raise ArgumentError(None, 'Die Zielspalte muss zur Zieltabelle gehören.')
    if cast_direction not in (0, 1, 2):
        raise ArgumentError(None, 'Der Parameter cast_direction darf nur die Werte 0, 1 oder 2 annehmen.')
    if target_column is not None and new_column_name is not None:
        raise ArgumentError(None, 'Wenn eine existierende Spalte als Ziel der Operation angegeben ist, kann hierfür kein neuer Name gewählt werden.')
    if attributes_to_join_on[0] not in target_table_data.columns or attributes_to_join_on[1] not in source_table_data.columns:
        raise ArgumentError(None, 'Das erste Attribut in attributes_to_join_on muss zur Zieltabelle gehören, das zweite zur Quelltabelle.')
    target_engine = target_table_data.engine
    source_engine = source_table_data.engine
    target_dialect = target_engine.dialect.name
    source_dialect = source_engine.dialect.name
    target_table = target_table_data.table_name
    target_attributes_to_select = target_table_data.columns
    if target_engine.url == source_engine.url or (target_dialect == 'mariadb' and source_dialect == 'mariadb' and target_engine.url.host == source_engine.url.host and target_engine.url.port == source_engine.url.port):
        joined_result, joined_column_names, unmatched_rows, cast_direction = join_tables_of_same_dialect_on_same_server([target_table_data, source_table_data], attributes_to_join_on, target_attributes_to_select, [source_column_to_insert], cast_direction, False)
    else:
        joined_result, joined_column_names, unmatched_rows = join_tables_of_different_dialects_dbs_or_servers([target_table_data, source_table_data], attributes_to_join_on, target_attributes_to_select, [source_column_to_insert], cast_direction, False)
        cast_direction = None
    if len(joined_result) + unmatched_rows[target_table] != target_table_data.total_row_count:
        raise MergeError('Mindestens einem Tupel der Zieltabelle konnte mehr als ein Tupel aus der Quelltabelle zugeordnet werden. Bitte wählen Sie Join-Attribute mit eindeutigen Werten.')
    
    
    source_data_type_info = source_table_data.data_type_info[source_column_to_insert]
    source_data_type_group = source_table_data.get_data_type(source_column_to_insert)
    source_data_type = source_table_data.get_data_type(source_column_to_insert)
    target_data_type_info = None
    target_data_type_group = None
    target_data_type = None
    if target_column is not None:
        target_data_type_info = target_table_data.data_type_info[target_column]
        target_data_type_group = target_table_data.get_data_type(target_column)
        target_data_type = target_table_data.get_data_type(target_column)
        if not target_data_type_group == source_data_type_group:
            raise MergeError(f'Die Datentypen {target_data_type} und {source_data_type} der Ziel- und der Quelltabelle sind nicht kompatibel.')
        elif target_data_type_group == 'text' and target_data_type_info['character_max_length'] < source_data_type_info['character_max_length']:
            raise MergeError(f'Die maximal erlaubte Zeichenanzahl in {target_column} reicht eventuell nicht aus, um alle Einträge des Attributs {source_column_to_insert} zu speichern.')
        elif target_data_type_group == 'integer':
            if (target_data_type == 'tinyint' and source_data_type != 'tinyint') or (target_data_type == 'smallint' and source_data_type not in ('smallint', 'tinyint')) or (target_data_type == 'mediumint' and source_data_type == 'bigint') or ((target_dialect == 'postgresql' and target_data_type != 'numeric') and (source_dialect == 'mariadb' and source_data_type == 'bigint' and 'is_unsigned' in source_data_type_info.keys())):
                raise MergeError(f'Der Wertebereich des Zieldatentyps {target_data_type} reicht eventuell nicht aus, um alle Einträge des Attributs {source_column_to_insert} zu speichern.')
    else:
        if target_dialect != source_dialect:
            if source_dialect == 'mariadb' and target_dialect == 'postgresql':
                if source_data_type_group == 'integer' and 'is_unsigned' in source_data_type_info.keys():
                    source_data_type = f'{source_data_type} unsigned'
                if source_data_type not in MariaToPostgresCompatibility.data_types.keys():
                    raise MergeError(f'Die Datentypen {target_data_type} und {source_data_type} der Ziel- und der Quelltabelle sind nicht kompatibel.')
                else:
                    target_data_type = MariaToPostgresCompatibility.data_types[source_data_type]
            elif source_dialect == 'postgresql' and target_dialect == 'mariadb':
                if source_data_type not in PostgresToMariaCompatibility.data_types.keys():
                    raise MergeError(f'Die Datentypen {target_data_type} und {source_data_type} der Ziel- und der Quelltabelle sind nicht kompatibel.')
                else:
                    target_data_type = PostgresToMariaCompatibility.data_types[source_data_type]
            target_data_type_info = {}
            target_data_type_info['data_type_group'] = source_data_type_group
            target_data_type_info['data_type'] = target_data_type

            for key in source_data_type_info.keys():
                if 'data_type' not in key:
                    target_data_type_info[key] = source_data_type_info[key]
        else:
            target_data_type_info = source_data_type_info
        
        if new_column_name == None:
            new_column_name = source_column_to_insert
        try:
            add_new_column(target_table_data, new_column_name, target_data_type_info, commit)
        except Exception as error:
            raise MergeError(f'Fehler beim Einfügen der neuen Spalte. {str(error)}')
    
    source_db = convert_string_if_contains_capitals_or_spaces(source_engine.url.database, source_dialect)
    source_table = convert_string_if_contains_capitals_or_spaces(source_table_data.table_name, source_dialect)
    target_db = convert_string_if_contains_capitals_or_spaces(target_engine.url.database, target_dialect)
    target_table = convert_string_if_contains_capitals_or_spaces(target_table, target_engine)
    target_attribute = convert_string_if_contains_capitals_or_spaces(new_column_name, target_dialect)
    source_attribute = convert_string_if_contains_capitals_or_spaces(source_column_to_insert, source_engine)
    target_join_attribute_data_type = target_table_data.get_data_type[attributes_to_join_on[0]]
    source_join_attribute_data_type = source_table_data.get_data_type[attributes_to_join_on[1]]
    target_join_attribute = convert_string_if_contains_capitals_or_spaces(attributes_to_join_on[0], target_engine)
    source_join_attribute = convert_string_if_contains_capitals_or_spaces(attributes_to_join_on[1], source_engine)

    update_query = None
    if source_engine.url == target_engine.url and target_dialect == 'postgresql':
        join_condition = f'WHERE {target_table}.{attributes_to_join_on[0]} = {source_table}.{attributes_to_join_on[1]}'
        if cast_direction == 1:
            join_condition = f'WHERE CAST({target_table}.{attributes_to_join_on[0]} AS {source_join_attribute_data_type}) = {source_table}.{attributes_to_join_on[1]}'
        elif cast_direction == 2:
            join_condition = f'WHERE {target_table}.{attributes_to_join_on[0]} = CAST({source_table}.{attributes_to_join_on[1]} AS {target_join_attribute_data_type})'
        
        update_query = f'UPDATE {target_table} SET {target_attribute} = {source_table}.{source_attribute} FROM {source_table} {join_condition}'
        # alternativ:
        # insert_query = f'MERGE INTO {target_table} t_tbl USING {source_table} s_tbl ON t_tbl.{target_join_attribute} = s_tbl.{source_join_attribute} WHEN MATCHED THEN UPDATE SET {target_attribute} = {source_attribute} WHEN NOT MATCHED THEN DO NOTHING'  
    elif (target_dialect == 'mariadb' and source_dialect == 'mariadb') and (target_engine.url.host == source_engine.url.host) and (target_engine.url.port == source_engine.url.port):
        if source_db != target_db:
            source_table = f'{source_db}.{source_table}'
            target_table = f'{target_db}.{target_table}'
        update_query = f'UPDATE {target_table} INNER JOIN (SELECT {source_join_attribute}, {source_column_to_insert} FROM {source_table}) sub_tbl'
        join_condition = f'ON {target_table}.{target_join_attribute} = sub_tbl.{source_join_attribute}'
        if cast_direction == 1:
            join_condition = f'ON CAST({target_table}.{target_join_attribute} AS {source_join_attribute_data_type}) = sub_tbl.{source_join_attribute}'
        elif cast_direction == 2:
            join_condition = f'ON {target_table}.{target_join_attribute} = CAST(sub_tbl.{source_join_attribute} AS {target_join_attribute_data_type})'
            # https://stackoverflow.com/questions/51977955/update-mariadb-table-using-a-select-query
        update_query = f'UPDATE {target_table} INNER JOIN (SELECT {source_join_attribute}, {source_column_to_insert} FROM {source_table}) sub_tbl ON {join_condition} SET {target_table}.{target_attribute} = sub_tbl.{source_column_to_insert}'
  
    else:
        escaped_pk_columns = []
        pk_indexes = []
        for index, key in enumerate(target_table_data.primary_keys):
            pk_indexes.append(index)
            escaped_pk_columns.append(convert_string_if_contains_capitals_or_spaces(key, target_dialect))
        new_column_index = len(joined_result) - 1
        
        update_query = f'UPDATE {target_table} SET {target_attribute} = CASE'
        for row in joined_result:
            condition = f'WHEN'
            for pk_index, key in enumerate(escaped_pk_columns):
                if pk_index != 0:
                    condition = f'{condition} AND'
                condition = f'{condition} {key} = {row[pk_index]}'
            update_query = f'{update_query} {condition} THEN CAST({row[new_column_index]} AS {target_data_type})'
        update_query = f'{update_query} ELSE {target_attribute} END'


    if update_query is not None:
        try:
            return convert_result_to_list_of_lists(execute_sql_query(target_engine, text(update_query), raise_exceptions = True, commit = commit))   
        except Exception as error:
            raise error
    

def add_new_column(table_meta_data:TableMetaData, column_name:str, target_column_data_type_info:dict[str:str], commit:bool = False):
    if 'data_type' not in target_column_data_type_info.keys():
        raise ArgumentError('Bitte geben Sie den Datentyp für die neue Spalte an.')
    if 'data_type_group' not in target_column_data_type_info.keys():
        raise ArgumentError('Bitte geben Sie die Datentypgruppe (z. B. integer, boolean, decimal, text, date) für die neue Spalte an.')
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, db_dialect)
    column_name = convert_string_if_contains_capitals_or_spaces(column_name, db_dialect)
    data_type = target_column_data_type_info['data_type']
    data_type_group = target_column_data_type_info['data_type_group']

    if data_type_group == 'integer':
        numeric_precision = target_column_data_type_info['numeric_precision']
        if db_dialect == 'mariadb':
            data_type = f'{data_type}({numeric_precision})'
            if 'is_unsigned' in target_column_data_type_info.keys():
                data_type = f'{data_type} unsigned'
    elif data_type_group == 'decimal':
        try:
            numeric_precision = int(target_column_data_type_info['numeric_precision'])
            numeric_scale = int(target_column_data_type_info['numeric_scale'])
        except (TypeError, ValueError):
            raise ArgumentError('Für Dezimalzahlen müssen die Werte numeric_precision und numeric_scale als ganze Zahlen angegeben sein.') 
        data_type = f'{data_type}({numeric_precision, numeric_scale})'
    elif data_type_group == 'text':
        if 'character_max_length' in target_column_data_type_info.keys() and target_column_data_type_info['character_max_length'] != None: 
            character_max_length = target_column_data_type_info['character_max_length']
            data_type = f'{data_type}({character_max_length})'
        else:
            raise ArgumentError('Für textbasierte Datentypen muss die maximal erlaubte Zeichenanzahl als ganze Zahl angegeben sein.')
    elif data_type_group == 'date':
        if 'datetime_precision' in target_column_data_type_info.keys() and target_column_data_type_info['datetime_precision'] != None: 
            datetime_precision = target_column_data_type_info['datetime_precision']
            data_type = f'{data_type}({datetime_precision})'
        else:
            raise ArgumentError('Für Datumsangaben muss der Wert datetime_precision als ganze Zahl angegeben sein.')
    unique_constraint = ''
    if target_column_data_type_info['is_unique']:
        unique_constraint = ' UNIQUE'
    add_query = text(f'ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type}{unique_constraint}')
    try:
        execute_sql_query(engine, add_query, raise_exceptions = True, commit = commit)
    except Exception as error:
        raise error
    else:
        return 0
    

def add_constraint_to_existing_column(table_meta_data:TableMetaData, column_name:str, constraint_type:str):
    if constraint_type not in ('nn', 'u', 'c'):
        raise ArgumentError('Mit dieser Funktion können ausschließlich Not-NULL-, Unique- oder Check-Constraints (nn, u bzw. c) zu einer Tabellenspalte hinzugefügt werden.')

def check_merging_compatibility(target_table_data:TableMetaData, source_table_data:TableMetaData, source_column_to_insert:str, target_column:str = None):
    source_column_info = source_table_data.data_type_info[source_column_to_insert]
    source_data_type = source_table_data.get_data_type(source_column_to_insert)
    source_dialect = source_table_data.engine.dialect.name
    target_dialect = target_table_data.engine.dialect.name
    if target_column != None:
        target_column_info = target_table_data.data_type_info[target_column]
        target_datatype = target_table_data.get_data_type(target_column)
        if source_column_info == target_column_info:
            return 0
    else:
        source_table_name = convert_string_if_contains_capitals_or_spaces(source_table_data.table_name)
        if source_dialect == 'mariadb':
            query = text(f'SHOW CREATE TABLE {source_table_name}')
            create_statement = str(execute_sql_query(source_table_data.engine, query).fetchone[1])
            create_statement = create_statement.partition('(')[-1]
            create_statement = remove_everything_after_last(')', create_statement)
        elif source_dialect == 'postgresql':
            return None

# https://stackoverflow.com/questions/18731028/remove-last-instance-of-a-character-and-rest-of-a-string
def remove_everything_after_last (needle, haystack, n:int = 1):
    while n > 0:
        idx = haystack.rfind(needle)
        if idx >= 0:
            haystack = haystack[:idx]
            n -= 1
        else:
            break
    return haystack 

def check_arguments_for_joining(table_meta_data:list[TableMetaData], attributes_to_join_on:list[str], attributes_to_select_1:list[str], attributes_to_select_2:list[str], cast_direction:int = None):
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
    elif cast_direction not in (0, 1, 2):
        raise ArgumentError(None, 'Bitte geben Sie den Wert 1 an, wenn das Verbindungsattribut von Tabelle 1 konvertiert werden soll, 2 für eine Konversion des Verbindungsattributs von Tabelle 2 und für das Auslassen von Konversionen den Wert None.')
    elif any([type(item) != TableMetaData for item in table_meta_data]):
        raise TypeError(None, 'Die Tabellenmetadaten müssen vom Typ TableMetaData sein.')

def list_attributes_to_select(attributes_to_select:list[str], dialect:str, table_name:str = None, db_name:str = None):
    attribute_string = ''
    for index, attribute in enumerate(attributes_to_select):
        query_attribute = attribute
        if table_name != None:
            if not table_name.startswith('"'):
                table_name = convert_string_if_contains_capitals_or_spaces(table_name, dialect)
            if db_name != None:
                if not db_name.startswith('"'):
                    db_name = convert_string_if_contains_capitals_or_spaces(db_name, dialect)
                table_name = f'{db_name}.{table_name}'
            if not query_attribute.startswith('"'):
                query_attribute = convert_string_if_contains_capitals_or_spaces(query_attribute, dialect)
            query_attribute = f'{table_name}.{query_attribute}'
        if index == 0:
            attribute_string = query_attribute
        else:
            attribute_string = f'{attribute_string} {query_attribute}'
        if len(attributes_to_select) > 1 and attribute != attributes_to_select[len(attributes_to_select) - 1]:
            attribute_string += ','
        print(attribute_string)
    return attribute_string

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
            constraint_query = f"SELECT i.table_name, a.attname FROM pg_constraint con JOIN pg_attribute a ON a.attnum = ANY(con.conkey) JOIN information_schema.columns i ON a.attname = i.column_name AND"
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
    compatibility_matrix = []
    compatibility_by_code = {}
    for column_name_1 in table_meta_data_1.columns:
        row_list = []
        for column_name_2 in table_meta_data_2.columns:
            full_dtype_info_1 = table_meta_data_1.data_type_info[column_name_1]
            full_dtype_info_2 = table_meta_data_2.data_type_info[column_name_2]
            dgroup_1 = table_meta_data_1.get_data_type_group(column_name_1)
            dgroup_2 = table_meta_data_2.get_data_type_group(column_name_2)
            dtype_1 = table_meta_data_1.get_data_type(column_name_1).lower()
            dtype_2 = table_meta_data_2.get_data_type(column_name_2).lower()
            # Code für Kompatibilität. 0 bei fehlender Kompatibilität; 1 bei voller Kompatibilität; 2 bei ggf. uneindeutigen Einträgen des Attributs;
            # 3, wenn ggf. Typkonversionen nötig sind; 4, wenn definitiv Typkonversionen notwendig sind. Durch Kombination können sich zudem die Werte
            # 5 für ggf. nicht eindeutige Werte mit ggf. nötigen Typkonversionen und 6 für ggf. nicht eindeutige Werte mit nötigen Typkonversionen ergeben.
            comp_code = 0
            if (full_dtype_info_1 == full_dtype_info_2 or dgroup_1 == dgroup_2) and (full_dtype_info_1['is_unique'] and full_dtype_info_2['is_unique']):
                comp_code = 1
            else:
                if not full_dtype_info_1['is_unique'] or not full_dtype_info_2['is_unique']:
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

    return compatibility_matrix, compatibility_by_code


def check_data_type_compatibility(table_meta_data_1:TableMetaData, table_meta_data_2:TableMetaData, table_1_is_target:bool):
    engine_1 = table_meta_data_1.engine
    engine_2 = table_meta_data_2.engine
    if  engine_1 == engine_2:
        engines = [engine_1]
    else:
        engines = [engine_1, engine_2]
    table_1 = table_meta_data_1.table_name
    table_2 = table_meta_data_2.table_name
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