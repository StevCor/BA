from argparse import ArgumentError
import re
from flask import render_template
from sqlalchemy import Engine
from ControllerClasses import TableMetaData
from model.databaseModel import check_data_type_meta_data, get_full_table_ordered_by_primary_key, get_primary_key_from_engine, get_row_count_from_engine
from model.oneTableModel import check_data_type_and_constraint_compatibility


def show_both_tables_separately(first_table_meta_data:TableMetaData, second_table_meta_data:TableMetaData, comp_by_code:dict, mode:str):
    db_name_1 = first_table_meta_data.engine.url.database
    db_name_2 = second_table_meta_data.engine.url.database
    db_dialects = []
    db_dialects.append(first_table_meta_data.engine.dialect.name)
    db_dialects.append(second_table_meta_data.engine.dialect.name)
    for index, dialect in enumerate(db_dialects):
        if dialect == 'mariadb':
            db_dialects[index] = 'MariaDB'
        elif dialect == 'postgresql':
            db_dialects[index] = 'PostgreSQL'
    table_1 = first_table_meta_data.table_name
    table_2 = second_table_meta_data.table_name
    data_1 = get_full_table_ordered_by_primary_key(first_table_meta_data)
    data_2 = get_full_table_ordered_by_primary_key(second_table_meta_data)
    table_columns_1 = first_table_meta_data.columns
    table_columns_2 = second_table_meta_data.columns
    if mode == 'merge':
        no_pk_columns_1 = [x for x in table_columns_1 if x not in first_table_meta_data.primary_keys]
        no_pk_columns_2 = [x for x in table_columns_2 if x not in second_table_meta_data.primary_keys]
        return render_template('two-tables.html', db_name_1 = db_name_1, db_dialects = db_dialects, table_name_1 = table_1, db_name_2 = db_name_2, table_name_2 = table_2, 
                               table_columns_1 = table_columns_1, data_1 = data_1, table_columns_2 = table_columns_2, data_2 = data_2, comp_by_code = comp_by_code, no_pk_columns_1 = no_pk_columns_1, no_pk_columns_2 = no_pk_columns_2, mode = mode)
    elif mode == 'compare':
        return render_template('two-tables.html', db_name_1 = db_name_1, db_dialects = db_dialects, table_name_1 = table_1, db_name_2 = db_name_2, table_name_2 = table_2, 
                               table_columns_1 = table_columns_1, data_1 = data_1, table_columns_2 = table_columns_2, data_2 = data_2, comp_by_code = comp_by_code, mode = mode)
    
# Überprüfung der Kompatibilität zwischen eingegebenem Wert und Attributsdatentyp
# Ausgabe des Wertes 0, wenn die Überprüfung erfolgreich war, sonst eine Fehlermeldung in Form eines Strings.
def check_validity_of_input_and_searched_value(table_meta_data:TableMetaData, input:str|None, column_name:str, old_value:str):
    data_type_group = table_meta_data.get_data_type_group(column_name)
    data_type = table_meta_data.get_data_type(column_name)
    if data_type_group == None:
        raise ArgumentError(None, f'In der Tabelle {table_meta_data.table_name} existiert keine Spalte mit dem Namen {column_name}.')
    elif input is None and not table_meta_data.data_type_info[column_name]['is_nullable']:
        return f'Das Attribut {column_name} erlaubt keine leeren bzw. NULL-Werte.'
    elif data_type_group == 'integer':
        if not re.match(r'^[-+]?([[1-9]\d*|0])(\.[0]+)?$', input):
            return f"Der gesuchte Wert \'{input}\' entspricht nicht dem Datentyp {data_type} des Attributs {column_name}.\n"
        else:
            try:
                int(input)
            except ValueError:
                return f"Der gesuchte Wert \'{input}\' entspricht nicht dem Datentyp {data_type} des Attributs {column_name}.\n"
    elif data_type_group == 'decimal':
        if not re.match(r'^[-+]?([[1-9]\d*|0])(\.[0]+)?$', input):
            return f"Der gesuchte Wert \'{input}\' entspricht nicht dem Datentyp {data_type} des Attributs {column_name}.\n"
        else:
            try:
                float(input)
            except ValueError:
                return f"Der gesuchte Wert \'{input}\' entspricht nicht dem Datentyp {data_type} des Attributs {column_name}.\n"
    elif data_type_group == 'text':
        char_max_length = table_meta_data.get_character_max_length(column_name)
        if char_max_length != None and len(input) > char_max_length:
            return f"Der eingegebene Wert \'{input}\' überschreitet die maximal erlaubte Zeichenanzahl {char_max_length} des Attributs {column_name}\n"
    elif data_type_group == 'boolean':
        if input not in (1, 0, True, False):
            return f"Der gesuchte Wert \'{input}\' entspricht nicht dem Datentyp {data_type} des Attributs {column_name}.\n"

    try:
        check_result = check_data_type_and_constraint_compatibility(table_meta_data, column_name, input, old_value)
    except Exception as error:
        if 'unique' in str(error).lower():
            return f'Der eingegebene Wert \'{input}\' verletzt eine \'UNIQUE\'-Constraint des Attributs {column_name}.\n'
        elif 'constraint' in str(error).lower():
            return f'Der eingegebene Wert \'{input}\' verletzt eine Constraint des Attributs {column_name}.\n'
        else:
            return f'Bei der Abfrage des Attributs {column_name} ist ein Datenbankfehler aufgetreten: {str(error)}.\n'
    else:
        return check_result
    
def update_TableMetaData_entries(engine:Engine, table_name:str):
    primary_keys = get_primary_key_from_engine(engine, table_name)
    data_type_info = check_data_type_meta_data(engine, table_name)
    total_row_count = get_row_count_from_engine(engine, table_name)
    return TableMetaData(engine, table_name, primary_keys, data_type_info, total_row_count)


