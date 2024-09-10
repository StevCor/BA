
from argparse import ArgumentError
import re
from sqlalchemy import bindparam, text
from ControllerClasses import TableMetaData
from model.SQLDatabaseError import DialectError, QueryError, UpdateError
from model.databaseModel import build_sql_condition, convert_result_to_list_of_lists, convert_string_if_contains_capitals_or_spaces, execute_sql_query, get_full_table_ordered_by_primary_key


### Funktionen für die Suche in einer Tabelle ###

def search_string(table_meta_data:TableMetaData, string_to_search:str, columns_to_search:list[str]):
    # Beziehen der benötigten Variablen aus table_meta_data: Engine, Dialektname und Tabellenname
    engine = table_meta_data.engine
    dialect = engine.dialect.name
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, dialect)
    # Sonderzeichen wie % müssen in regulären Ausdrücken dialektspezifisch gekennzeichnet werden, da sie darin eine besondere Bedeutung haben 
    string_to_search = escape_string(dialect, string_to_search)
    # Damit die Suche in MariaDB und PostgreSQL Groß- und Kleinschreibung ignoriert, müssen in MariaDB der Operator 'LIKE' und der Datentyp 'CHAR'
    # zur Konversion nicht textbasierter Datentypen verwendet werden, in PostgreSQL 'ILIKE' bzw. 'TEXT'.
    operator, cast_data_type = set_matching_operator_and_cast_data_type(dialect)
    # Auch das Verbinden von Strings erfordert unterschiedliche Vorgehensweisen. In PostgreSQL können Strings mit dem Operator '||' verbunden
    # werden, in MariaDB müssen die beiden Strings hingegen der Funktion 'CONCAT' übergeben werden.
    concatenated_string = get_concatenated_string_for_matching(dialect, 'string_to_search')
    # Aufbau der Bedingung für die Suche
    sql_condition = 'WHERE'
    # Iteration durch alle Attribute in der ausgewählten Tabelle
    for column_name in columns_to_search:
        # Sie werden mit doppelten Anführungszeichen versehen, wenn sie Großbuchstaben (PostgreSQL) oder Leerzeichen enthalten (MariaDB und PostgreSQL).
        attribute_to_search = convert_string_if_contains_capitals_or_spaces(column_name, dialect).strip()
        # Wenn sie nicht textbasiert sind, ...
        if table_meta_data.get_data_type_group(column_name) != 'text':
            # ... werden sie in den dialektspezifischen Textdatentyp konvertiert.
            attribute_to_search = f'CAST({attribute_to_search} AS {cast_data_type})'
        if sql_condition == 'WHERE':
            # Anängen des zu durchsuchenden Attributs und des regulären Ausdrucks für die Suche an die Bedingung 
            sql_condition = f"{sql_condition} {attribute_to_search} {operator} {concatenated_string}"
        # Handelt es sich nicht um das erste Attribut, ...
        else:
            # ... wird hierbei noch der Operator 'OR' ergänzt.
            sql_condition = f"{sql_condition} OR {attribute_to_search} {operator} {concatenated_string}"
    # Zusammenfügen der Abfrage für die Suche 
    query = text(f"SELECT * FROM {table_name} {sql_condition}")
    # Aufbau des Parameter-Dictionarys
    params = {'string_to_search': string_to_search}
    # Wenn der SQL-Dialekt entweder PostgreSQL oder MariaDB ist, ...
    if dialect == 'postgresql' or dialect == 'mariadb':
        # ... wird die Abfrage ausgeführt und das Ergebnis in eine Liste von Listen konvertiert.
        result = convert_result_to_list_of_lists(execute_sql_query(engine, query, params))
    else:
        # Anderenfalls wird eine Meldung ausgegeben, dass der gewählte SQL-Dialekt nicht unterstützt wird.
        raise DialectError(f'Der SQL-Dialekt {dialect} wird nicht unterstützt.')
    return result


### Funktionen für das Ersetzen von (Teil-)Strings ###

def get_replacement_information(table_meta_data:TableMetaData, affected_attributes_and_positions:list[tuple[str, int:0|1]], old_value:str, replacement:str):
    cols_and_dtypes = table_meta_data.data_type_info
    if len(affected_attributes_and_positions) != len(cols_and_dtypes.keys()):
        raise ArgumentError(None, 'Für alle Attribute der Tabelle muss angegeben sein, ob sie von der Änderung betroffen sein können oder nicht.')
    if any([x[1] != 0 and x[1] != 1 for x in affected_attributes_and_positions]):
        raise ArgumentError(None, 'Kann ein Attribut von der Änderung betroffen sein, muss dies durch den Wert 1 in der Liste gekennzeichnet sein. Anderenfalls sollte dort der Wert 0 stehen.')
    engine = table_meta_data.engine
    table_name = table_meta_data.table_name
    primary_keys = table_meta_data.primary_keys
    affected_attributes = []
    positions = []
    for item in affected_attributes_and_positions:
        print('item: ', item)
        if item[1]:
            affected_attributes.append(item[0])
        positions.append(item[1])
        
    occurrence_dict = {}
    if len(affected_attributes) < 1:
        raise ArgumentError('Es muss mindestens ein Attribut angegeben sein, dessen Werte bearbeitet werden sollen.')
    elif len(affected_attributes) > 1:
        unaltered_table = get_full_table_ordered_by_primary_key(table_meta_data, convert = False)
        all_attributes = list(unaltered_table.keys())
        primary_key_indexes = []
        for index, key in enumerate(all_attributes):
            if key in primary_keys:
                primary_key_indexes.append(index)
        unaltered_table = convert_result_to_list_of_lists(unaltered_table)
        table_with_full_replacement = replace_all_string_occurrences(table_meta_data, affected_attributes, old_value, replacement, commit = False)
        row_nos_old_and_new_values = get_indexes_of_affected_attributes_for_replacing(table_meta_data, old_value, affected_attributes)
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
    else:
        attribute_with_full_replacement = replace_all_string_occurrences(table_meta_data, affected_attributes, old_value, replacement, commit = False)
        affected_row_nos_and_unaltered_entries = get_row_number_of_affected_entries(table_meta_data, affected_attributes, [old_value], 'replace', convert = False)
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
    
    return row_nos_old_and_new_values, occurrence_dict



# alle Vorkommen eines Teilstrings ersetzen
def replace_all_string_occurrences(table_meta_data:TableMetaData, column_names:list, string_to_replace:str, replacement_string:str, commit:bool = False):
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, db_dialect)
    string_to_replace = escape_string(db_dialect, string_to_replace)
    replacement_string = escape_string(db_dialect, replacement_string)
    primary_keys = ', '.join(table_meta_data.primary_keys)
    update_params = {}
    flag = ''
    if db_dialect == 'postgresql':
        flag = ", 'g'"
    query = f'UPDATE {table_name} SET'
    for index, column_name in enumerate(column_names):
        column_name = convert_string_if_contains_capitals_or_spaces(column_name, db_dialect)
        data_type_group = table_meta_data.get_data_type_group(column_name)
        if data_type_group != 'text':
            query = f"{query} {column_name} = :new_value_{str(index)}"
            if data_type_group == 'integer':
                update_params[f'new_value_{str(index)}'] = int(replacement_string)
            elif data_type_group == 'decimal':
                update_params[f'new_value_{str(index)}'] = float(replacement_string)
            else:
                update_params[f'new_value_{str(index)}'] = replacement_string
        else:
            query = f"{query} {column_name} = regexp_replace({column_name}, :string_to_replace, :replacement_string{flag})"
            if 'string_to_replace' not in update_params.keys():
                update_params['string_to_replace'] = string_to_replace
                update_params['replacement_string'] = replacement_string
        if column_name != column_names[len(column_names)-1]:
            query = f'{query},' 
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

def get_indexes_of_affected_attributes_for_replacing(table_meta_data:TableMetaData, old_value:str, affected_attributes:list = None):
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, db_dialect)
    cols_and_dtypes = table_meta_data.data_type_info
    primary_keys = table_meta_data.primary_keys
    string_to_replace = escape_string(db_dialect, old_value)
    params_dict = {'old_value': string_to_replace}
    keys = ', '.join(primary_keys)
    query = 'SELECT'
    case_selected_attribute = 'THEN 1 ELSE 0 END'
    case_nonselected_attribute = '0'
    operator, cast_data_type = set_matching_operator_and_cast_data_type(db_dialect)
    concatenated_string = get_concatenated_string_for_matching(db_dialect, 'old_value')
    condition = f"{operator} {concatenated_string}"
    for index, key in enumerate(cols_and_dtypes.keys()):
        if affected_attributes == None or (affected_attributes != None and key in affected_attributes):
            if table_meta_data.get_data_type_group(key) != 'text': #[1]
                query = f'{query} CASE WHEN CAST({key} AS {cast_data_type}) {condition} {case_selected_attribute}'
            else:
                query = f'{query} CASE WHEN {key} {condition} {case_selected_attribute}'
        else: 
            query = f'{query} {case_nonselected_attribute}'
        if index < len(cols_and_dtypes.keys())-1:
            query = f'{query},'
    query = text(f'{query} FROM {table_name} ORDER BY {keys}')
    result = execute_sql_query(engine, query, params_dict)

    row_ids = dict()
    for index, row in enumerate(result):
        if sum(row) != 0:
            # index + 1, da die SQL-Funktion ROW_NUMBER() ab 1 zählt; Listenkonversion, damit die Einträge verändert werden können
            row_ids[index+1] = list(row)
    return row_ids

def replace_some_string_occurrences(table_meta_data:TableMetaData, occurrences_dict:dict, string_to_replace:str, replacement_string:str, commit:bool = False):
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, db_dialect)
    string_to_replace = escape_string(db_dialect, string_to_replace)
    replacement_string = escape_string(db_dialect, replacement_string)
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
        if db_dialect == 'postgresql':
            flag = ", 'g'"
        if len(primary_key) != len(primary_key_attributes):
            raise QueryError('Es müssen gleich viele Spaltennamen und Attributwerte für den Primärschlüssel angegeben werden.')
        affected_attribute = row['affected_attribute']
        data_type_group = table_meta_data.get_data_type_group(affected_attribute)
        if data_type_group != 'text':
            query = f"{query} {affected_attribute} = :new_value"
            if data_type_group == 'integer':
                update_params[f'new_value'] = int(replacement_string)
            elif data_type_group == 'decimal':
                update_params[f'new_value'] = float(replacement_string)
            else:
                update_params[f'new_value'] = replacement_string
        else:
            query = f"{query} {affected_attribute} = regexp_replace({affected_attribute}, :string_to_replace, :replacement_string{flag})"
            update_params['string_to_replace'] = string_to_replace
            update_params['replacement_string'] = replacement_string
        for index, key in enumerate(primary_key_attributes):
            update_params[key] = primary_key[index]
        condition = build_sql_condition(tuple(primary_key_attributes), db_dialect, 'AND')
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
    

### Funktionen für das Vereinheitlichen von Datenbankeinträgen

def get_unique_values_for_attribute(table_meta_data:TableMetaData, attribute_to_search:str):
    if type(table_meta_data) != TableMetaData:
        raise ArgumentError(None, 'Der erste übergebene Parameter muss vom Typ TableMetaData sein.')
    engine = table_meta_data.engine
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, engine.dialect.name)
    attribute_to_search = convert_string_if_contains_capitals_or_spaces(attribute_to_search, engine.dialect.name)
    query = text(f'SELECT DISTINCT {attribute_to_search}, COUNT(*) AS Eintragsanzahl FROM {table_name} GROUP BY {attribute_to_search}')
    return convert_result_to_list_of_lists(execute_sql_query(engine, query))

def update_to_unify_entries(table_meta_data:TableMetaData, attribute_to_change:str, old_values:list, new_value:str, commit:bool):
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    table_name = table_meta_data.table_name
    query = f'UPDATE {convert_string_if_contains_capitals_or_spaces(table_name, db_dialect)} SET {convert_string_if_contains_capitals_or_spaces(attribute_to_change, db_dialect)} = :new_value'
    cols_and_dtypes = table_meta_data.data_type_info
    condition_dict = {}
    print(old_values)
    for index, key in enumerate(cols_and_dtypes.keys()):
        if key == attribute_to_change:
            data_type_group = table_meta_data.get_data_type_group(key)
            if data_type_group == 'integer':
                new_value = int(new_value)
                for index, item in enumerate(old_values):
                    old_values[index] = int(item)
                break
            elif data_type_group == 'decimal':
                new_value = float(new_value)
                for item in enumerate(old_values):
                    old_values[index] = float(item)
                break 
    condition_dict['new_value'] = new_value
    condition = 'WHERE'
    for index, value in enumerate(old_values):
        if index == 0:
            condition = f'{condition} {convert_string_if_contains_capitals_or_spaces(attribute_to_change, db_dialect)} = :value_{str(index)}'
        else:
            condition = f'{condition} OR {convert_string_if_contains_capitals_or_spaces(attribute_to_change, db_dialect)} = :value_{str(index)}'
        condition_dict['value_' + str(index)] = value
    query = text(f'{query} {condition}')
    print('UPDATE-Anweisung:', query)
    return execute_sql_query(engine, query, condition_dict, True, commit)


### Hilfsfunktionen, die an mehreren Stellen verwendet werden (können) ###

def check_data_type_and_constraint_compatibility(table_meta_data:TableMetaData, column_name:str, input:str|int|float|bool, old_value:str):
    if type(input) not in (str, int, float, bool):
        print('Datentyp kann nicht überprüft werden.')
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, db_dialect)
    column_name = convert_string_if_contains_capitals_or_spaces(column_name, db_dialect)
    update_params = {}
    update_params['old_value'] = old_value
    pre_query = f'SELECT {column_name} FROM {table_name} WHERE'
    operator, cast_data_type = set_matching_operator_and_cast_data_type(db_dialect) 
    string_to_search = get_concatenated_string_for_matching(db_dialect, 'old_value')
    if table_meta_data.get_data_type_group(column_name) == 'text':
        pre_query = f"{pre_query} {column_name} {operator} {string_to_search} LIMIT 1"
    else:
        pre_query = f"{pre_query} CAST({column_name} AS {cast_data_type}) {operator} {string_to_search} LIMIT 1"
    print(pre_query)
    try:
        result = convert_result_to_list_of_lists(execute_sql_query(engine, text(pre_query), update_params, raise_exceptions = True, commit = False))
    except Exception as error:
        raise error
    else:
        if len(result) == 0 or result == None:
            return f'Der gesuchte Wert \'{old_value}\' kommt im Attribut {column_name} nicht vor.\n'
        condition_value = result[0][0]

        query = f'UPDATE {table_name} SET {column_name}'
        condition = f'WHERE {column_name} = :condition_value'
        flag = ''
        if db_dialect == 'postgresql':
            flag = ", 'g'"
        if table_meta_data.get_data_type_group(column_name) == 'text':
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


def get_row_number_of_affected_entries(table_meta_data:TableMetaData, affected_attributes:list[str], old_values:list[str], mode:str, convert:bool = True):
    if not mode == 'replace' and not mode == 'unify':
        raise ArgumentError(None, 'Nur die Modi \'replace\' und \'unify\' werden unterstützt.')
    elif mode == 'unify' and len(affected_attributes) != 1:
        raise ArgumentError(None, 'Im Modus \'unify\' kann nur ein Attribut bearbeitet werden.')
    elif mode == 'replace' and len(old_values) != 1:
        raise ArgumentError(None, 'Im Modus \'replace\' kann nur ein Wert ersetzt werden.')
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    table_name = table_meta_data.table_name
    cols_and_dtypes = table_meta_data.data_type_info
    primary_keys = table_meta_data.primary_keys
    operator, cast_data_type = set_matching_operator_and_cast_data_type(db_dialect)
    key_for_ordering = ', '.join(primary_keys)
    columns_to_select = '*'
    if db_dialect == 'mariadb':
        for key in cols_and_dtypes.keys():
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
        concat_string = get_concatenated_string_for_matching(db_dialect, 'old_value')
        for index, attribute in enumerate(affected_attributes):
            attribute_to_search = convert_string_if_contains_capitals_or_spaces(attribute, db_dialect).strip()
            if table_meta_data.get_data_type_group(attribute) != 'text':
                attribute_to_search = f'CAST(sub.{attribute_to_search} AS {cast_data_type})'
            else:
                attribute_to_search = f'sub.{attribute_to_search}'
            if index == 0:
                condition = f"{condition} {attribute_to_search} {operator} {concat_string}"
            else:
                condition = f"{condition} OR {attribute_to_search} {operator} {concat_string}"

    elif mode == 'unify':
        affected_attribute = affected_attributes[0]
        data_type_group = table_meta_data.get_data_type_group(affected_attribute)
        for index, value in enumerate(old_values):
            if data_type_group == 'text':
                condition_params['value_' + str(index)] = value
            elif data_type_group == 'integer':
                condition_params['value_' + str(index)] = int(value)
            elif data_type_group == 'decimal':
                condition_params['value_' + str(index)] = float(value)
            
            if index == 0:
                condition = f"{condition} sub.{convert_string_if_contains_capitals_or_spaces(affected_attribute, db_dialect)} = :{'value_' + str(index)}"
            else:
                condition = f"{condition} OR sub.{convert_string_if_contains_capitals_or_spaces(affected_attribute, db_dialect)} = :{'value_' + str(index)}"
    query = text(f'{query} {condition}')
    result = execute_sql_query(engine, query, condition_params)
    if convert:
        return convert_result_to_list_of_lists(result)
    else:
        return result

def set_matching_operator_and_cast_data_type(db_dialect:str):
    operator = ''
    cast_data_type = ''
    if db_dialect == 'mariadb':
        operator = 'LIKE'
        cast_data_type = 'CHAR'
    elif db_dialect == 'postgresql':
        operator = 'ILIKE'
        cast_data_type = 'TEXT'
    return operator, cast_data_type

def get_concatenated_string_for_matching(db_dialect:str, search_parameter_name:str):
    # Sicherstellung, dass der angegebene Dialekt unterstützt wird
    if db_dialect not in ('mariadb', 'postgresql'):
        raise DialectError(None, f'Der SQL-Dialekt {db_dialect} wird nicht unterstützt.')
    # Version des Ausdrucks für MariaDB
    if db_dialect == 'mariadb':
        return f"CONCAT('%', CONCAT(:{search_parameter_name}, '%'))"
    # Version des Ausdrucks für PostgreSQL
    else:
        return f"'%' || :{search_parameter_name} || '%'"
    
def escape_string(db_dialect:str, string:str):
    if db_dialect == 'postgresql':
        string = string.replace('\\', '\\\\').replace('%', '\%').replace('_', '\_').replace("'", "\'").replace('"', '\"')
    elif db_dialect == 'mariadb':
        string = string.replace('\\', '\\\\\\\\').replace('%', '\%').replace('_', '\_').replace("'", "\'").replace('"', '\"')
    print(string)
    return string