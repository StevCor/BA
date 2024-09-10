from argparse import ArgumentError
import re
from flask import render_template
from sqlalchemy import Engine
from ControllerClasses import TableMetaData
from model.databaseModel import get_data_type_meta_data, get_full_table_ordered_by_primary_key, get_primary_key_from_engine, get_row_count_from_engine
from model.oneTableModel import check_data_type_and_constraint_compatibility


def show_both_tables_separately(first_table_meta_data:TableMetaData, second_table_meta_data:TableMetaData, comp_by_code:dict, mode:str):
    """Bezieht die Informationen zweier Datenbanktabellen, die für ihre Anzeige in two-tables.html benötigt werden, und ruft Letztere auf.
    
    first_table_meta_data: TableMetaData-Objekt für die zuerst anzuzeigende Tabelle

    second_table_meta_data: TableMetaData-Objekt für die Tabelle, die als Zweites angezeigt werden soll

    comp_by_code: Dictionary mit Schlüsseln der Werte 0 (keine Kompatibilität), 1 (vollständige Kompatibilität), 2 (uneindeutig), 
    5 (uneindeutig und ggf. Typkonversion nötig) und/oder 6 (uneindeutig und definitiv Typkonversion nötig)  

    mode: 'compare' oder 'merge', da für die Optionen unterschiedliche Variablen für die Anzeige benötigt werden."""

    ### Anlegen der Variablen für leichteren Zugriff mit abgekürzten Namen ###
    db_name_1 = first_table_meta_data.engine.url.database
    db_name_2 = second_table_meta_data.engine.url.database
    db_dialects = []
    db_dialects.append(first_table_meta_data.engine.dialect.name)
    db_dialects.append(second_table_meta_data.engine.dialect.name)
    table_1 = first_table_meta_data.table_name
    table_2 = second_table_meta_data.table_name
    table_columns_1 = first_table_meta_data.columns
    table_columns_2 = second_table_meta_data.columns
    ### Verschönerung der Dialekte für die Anzeige ###
    for index, dialect in enumerate(db_dialects):
        if dialect == 'mariadb':
            db_dialects[index] = 'MariaDB'
        elif dialect == 'postgresql':
            db_dialects[index] = 'PostgreSQL'
    ### Beziehen der Daten für beide Tabellen ###
    data_1 = get_full_table_ordered_by_primary_key(first_table_meta_data)
    data_2 = get_full_table_ordered_by_primary_key(second_table_meta_data)
    # Für die Attributsübertragung ...
    if mode == 'merge':
        # ... wird die Information benötigt, welche der Attribute in beiden Tabellen keine Primärschlüssel sind, da nur diese als Zielattribut ausgewählt werden können
        no_pk_columns_1 = [x for x in table_columns_1 if x not in first_table_meta_data.primary_keys]
        no_pk_columns_2 = [x for x in table_columns_2 if x not in second_table_meta_data.primary_keys]
        return render_template('two-tables.html', db_name_1 = db_name_1, db_dialects = db_dialects, table_name_1 = table_1, db_name_2 = db_name_2, table_name_2 = table_2, 
                               table_columns_1 = table_columns_1, data_1 = data_1, table_columns_2 = table_columns_2, data_2 = data_2, comp_by_code = comp_by_code, no_pk_columns_1 = no_pk_columns_1, no_pk_columns_2 = no_pk_columns_2, mode = mode)
    # Für den Vergleich werden lediglich die oben gewonnenen Daten übergeben.
    elif mode == 'compare':
        return render_template('two-tables.html', db_name_1 = db_name_1, db_dialects = db_dialects, table_name_1 = table_1, db_name_2 = db_name_2, table_name_2 = table_2, 
                               table_columns_1 = table_columns_1, data_1 = data_1, table_columns_2 = table_columns_2, data_2 = data_2, comp_by_code = comp_by_code, mode = mode)
    

def check_validity_of_input_and_searched_value(table_meta_data:TableMetaData, input:str|None, column_name:str, old_value:str):
    """Überprüfung der Kompatibilität zwischen eingegebenem Wert und Attributsdatentyp

    table_meta_data: TableMetaData-Objekt der betroffenen Tabelle

    input: neu einzutragender Wert

    column_name: Name des zu überprüfenden Attributs

    old_value: Wert, der ersetzt werden soll

    Ausgabe des Wertes 0, wenn die Überprüfung erfolgreich war, sonst eine Fehlermeldung in Form eines Strings."""

    data_type_group = table_meta_data.get_data_type_group(column_name)
    data_type = table_meta_data.get_data_type(column_name)
    # Überprüfung, ob gültige Datentypinformationen bezogen werden können
    if data_type_group == None:
        raise ArgumentError(None, f'In der Tabelle {table_meta_data.table_name} existiert keine Spalte mit dem Namen {column_name}.')
    # Sicherstellung, dass NULL-Werte nur eingetragen werden können, wenn sie erlaubt sind
    elif input is None and not table_meta_data.data_type_info[column_name]['is_nullable']:
        return f'Das Attribut {column_name} erlaubt keine leeren bzw. NULL-Werte.'
    # Überprüfung für ganze Zahlen ...
    elif data_type_group == 'integer':
        # ... anhand eines regulären Ausdrucks ...
        if not re.match(r'^[-+]?([[1-9]\d*|0])(\.[0]+)?$', input):
            return f"Der gesuchte Wert \'{input}\' entspricht nicht dem Datentyp {data_type} des Attributs {column_name}.\n"
        else:
            try:
                # ... und anhand von Python-Typkonversion.
                int(input)
            except ValueError:
                return f"Der gesuchte Wert \'{input}\' entspricht nicht dem Datentyp {data_type} des Attributs {column_name}.\n"
    # Überprüfung für Dezimalzahlen ...
    elif data_type_group == 'decimal':
        # ... anhand eines regulären Ausdrucks ...
        if not re.match(r'^[-+]?([[1-9]\d*|0])(\.[0]+)?$', input):
            return f"Der gesuchte Wert \'{input}\' entspricht nicht dem Datentyp {data_type} des Attributs {column_name}.\n"
        else:
            try:
                # ... und anhand von Python-Typkonversion.
                float(input)
            except ValueError:
                return f"Der gesuchte Wert \'{input}\' entspricht nicht dem Datentyp {data_type} des Attributs {column_name}.\n"
    elif data_type_group == 'text':
        # Für textbasierte Datentypen wird die maximal erlaubte Zeichenlänge überprüft.
        char_max_length = table_meta_data.get_character_max_length(column_name)
        # Wenn hierfür ein Wert besteht, muss die Länge des neuen Eintrags darunter liegen. 
        if char_max_length != None and len(input) > char_max_length:
            return f"Der eingegebene Wert \'{input}\' überschreitet die maximal erlaubte Zeichenanzahl {char_max_length} des Attributs {column_name}\n"
    # Als Werte für Boolean-Attribute werden nur 1 und 0 bzw. True und False akzeptiert.
    elif data_type_group == 'boolean':
        if input not in (1, 0, True, False):
            return f"Der gesuchte Wert \'{input}\' entspricht nicht dem Datentyp {data_type} des Attributs {column_name}.\n"
        
    # Wenn all diese Überprüfungen erfolgreich waren, wird ein Eintrag in die Datenbank simuliert, um Informationen zu bestehenden Constraints zu erhalten.
    try:
        check_result = check_data_type_and_constraint_compatibility(table_meta_data, column_name, input, old_value)
    # Treten hierbei Fehler auf, ...
    except Exception as error:
        # ... werden in der Meldung z. B. explizit UNIQUE- ...
        if 'unique' in str(error).lower():
            return f'Der eingegebene Wert \'{input}\' verletzt eine \'UNIQUE\'-Constraint des Attributs {column_name}.\n'
        # ... und andere Constraints erwähnt.
        elif 'constraint' in str(error).lower():
            return f'Der eingegebene Wert \'{input}\' verletzt eine Constraint des Attributs {column_name}.\n'
        # Bei anderen Fehlern wird die Meldung zusätzlich zu dem Hinweis auf einen Datenbankfehler ausgegeben.
        else:
            return f'Bei der Abfrage des Attributs {column_name} ist ein Datenbankfehler aufgetreten: {str(error)}.\n'
    else:
        # Ohne Fehler wird der Wert 0 zurückgegeben.
        return check_result
    
def update_TableMetaData_entries(engine:Engine, table_name:str):
    """Funktion zum Verändern bestehender Attribute eines TableMetaData-Objektes.
    
    engine: die sqlalchemy.Engine zu der Datenbank, der die Tabelle angehört

    table_name: Name der Tabelle, deren TableMetaData-Objekt aktualisiert werden soll

    Gibt ein neues TableMetaData-Objekt mit den veränderten Werten aus."""

    # Beziehen der Primärschlüssel, ...
    primary_keys = get_primary_key_from_engine(engine, table_name)
    # ... der Datentypinformationen ...
    data_type_info = get_data_type_meta_data(engine, table_name)
    # ... und der Tupelanzahl.
    total_row_count = get_row_count_from_engine(engine, table_name)
    # Ausgabe des neuen Objektes
    return TableMetaData(engine, table_name, primary_keys, data_type_info, total_row_count)


