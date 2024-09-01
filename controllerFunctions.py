from argparse import ArgumentError
import re

from sqlalchemy import Engine
from ControllerClasses import TableMetaData
from model.databaseModel import check_data_type_meta_data, get_primary_key_from_engine, get_row_count_from_engine
from model.oneTableModel import check_data_type_and_constraint_compatibility

# gibt eine ganze Zahl aus, je nach Status der Überprüfung
# 0: Prüfung erfolgreich, Daten können eingefügt werden
# 1: eingegebener Wert kann nicht in den Datentyp des durchsuchten Attributs konvertiert werden
# 2: gesuchter Wert entspricht nicht dem Datentyp des durchsuchten Attributs
# 3: eingegebener Text überschreitet die in der Datenbank maximal erlaubte Zeichenanzahl
# 4: keine Einträge für diese Suche
# 5: aktualisierte Daten würden eine 'unique'-Constraint verletzen
# 6: aktualisierte Daten verletzen eine Constraint
# 7: anderer Fehler bei der Datenbankabfrage
def check_validity_of_input_and_searched_value(table_meta_data:TableMetaData, input:str, column_name:str, old_value:str):
    data_type_group = table_meta_data.get_data_type_group(column_name)
    if data_type_group == None:
        raise ArgumentError(None, f'In der Tabelle {table_meta_data.table_name} existiert keine Spalte mit dem Namen {column_name}.')
    elif data_type_group == 'integer':
        if not re.match(r'^[-+]?([[1-9]\d*|0])(\.[0]+)?$', input):
            return 1
        else:
            try:
                int(input)
            except ValueError:
                return 2
    elif data_type_group == 'decimal':
        if not re.match(r'^[-+]?([[1-9]\d*|0])(\.[0]+)?$', input):
            return 1
        else:
            try:
                float(input)
            except ValueError:
                return 2
    elif data_type_group == 'text':
        char_max_length = table_meta_data.get_character_max_length(column_name)
        if char_max_length != None and len(input) > char_max_length:
            return 3
    elif data_type_group == 'boolean':
        if input not in (1, 0, True, False):
            return 2

    try:
        check_result = check_data_type_and_constraint_compatibility(table_meta_data, column_name, input, old_value)
    except Exception as error:
        if 'unique' in str(error).lower():
            print(error)
            return 5
        elif 'constraint' in str(error).lower():
            return 6
        else:
            return 7
    else:
        return check_result
    
def update_TableMetaData_entries(table_meta_data:TableMetaData, engine:Engine, table_name:str):
    table_meta_data.primary_keys = get_primary_key_from_engine(engine, table_name)
    table_meta_data.data_type_info = check_data_type_meta_data(engine, table_name)
    table_meta_data.total_row_count = get_row_count_from_engine(engine, table_name)