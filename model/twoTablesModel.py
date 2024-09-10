from argparse import ArgumentError
from dateutil import parser
from math import ceil
from sqlalchemy import Engine, bindparam, text
from ControllerClasses import TableMetaData
from model.CompatibilityClasses import MariaInt, PostgresInt, get_int_value_by_dialect_name, MariaToPostgresCompatibility, PostgresToMariaCompatibility
from model.databaseModel import get_primary_key_from_engine, convert_result_to_list_of_lists, execute_sql_query, convert_string_if_contains_capitals_or_spaces
from model.SQLDatabaseError import DialectError, MergeError, QueryError



def join_tables_of_same_dialect_on_same_server(table_meta_data:list[TableMetaData], attributes_to_join_on:list[str], attributes_to_select_1:list[str], attributes_to_select_2:list[str], cast_direction:int = 0, full_outer_join:bool = False, add_table_names_to_column_names:bool = True, return_cast_direction:bool = False):
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
    
    ### Vorbereitung der Namen der Datenbankbestandteile für eindeutigen Zugriff ###
    # Versehen der Tabellennamen mit Trennzeichen, falls nötig
    table_1 = f'{convert_string_if_contains_capitals_or_spaces(table_meta_data[0].table_name, dialect_1)}'
    table_2 = f'{convert_string_if_contains_capitals_or_spaces(table_meta_data[1].table_name, dialect_2)}'
    # Verbindung des Tabellennamens mit den ggf. mit Trennzeichen versehenen Join-Attributen zur eindeutigen Identifizierung
    join_attribute_1 = f'{table_1}.{convert_string_if_contains_capitals_or_spaces(attributes_to_join_on[0], dialect_1)}'
    join_attribute_2 = f'{table_2}.{convert_string_if_contains_capitals_or_spaces(attributes_to_join_on[1], dialect_2)}'
    
    db_name_1 = None
    db_name_2 = None
    # Liegen die Tabellen in zwei Datenbanken, werden zudem deren Namen für die eindeutige Tabellenidentifizierung benötigt. 
    if table_meta_data[0].engine.url.database != table_meta_data[1].engine.url.database:
        db_name_1 = table_meta_data[0].engine.url.database
        db_name_2 = table_meta_data[1].engine.url.database
    
    
    ### Festlegen der auszuwählenden Attribute für den Join ###
    # Trennzeichen, das zwischen die Attribute der ersten und der zweiten Tabelle gesetzt wird
    delimiter = ','
    # Falls aus der ersten Tabelle keine Attribute ausgewählt wurden, ...
    if len(attributes_to_select_1) == 0:
        # ... wird es auf einen leeren String gesetzt, ...
        delimiter = ''
        # ... ebenso wie die Auflistung der Attribute aus der ersten Tabelle.
        attributes_table_1 = ''
    else:
        # Anderenfalls werden die Attribute durch Kommas getrennt aufgelistet.
        attributes_table_1 = list_attributes_to_select(attributes_to_select_1, dialect_1, table_1, db_name_1)
    # Falls aus der ersten Tabelle keine Attribute ausgewählt wurden, ...
    if len(attributes_to_select_2) == 0:
        # ... wird das Trennzeichen ebenfalls auf einen leeren String gesetzt.
        delimiter = ''
    # Anlegen der Anfrage für den Join
    join_query = f'SELECT {attributes_table_1}{delimiter}'
    # Wenn Attribute aus der zweiten Tabelle angezeigt werden sollen, ...
    if len(attributes_to_select_2) > 0:
        # ... werden diese analog zu den Attributen der ersten Tabelle aufgelistet ...
        attributes_table_2 = list_attributes_to_select(attributes_to_select_2, dialect_2, table_2, db_name_2)
        # ... und der Anfrage angehängt.
        join_query = f'{join_query} {attributes_table_2}'
    
    ### Typkonversion ###
    # Anlegen der Variablen für die Beurteilung, ob eine Typkonversion nötig ist
    data_type_group_1 = table_meta_data[0].get_data_type_group(attributes_to_join_on[0])
    data_type_group_2 = table_meta_data[1].get_data_type_group(attributes_to_join_on[1])
    data_type_1 = table_meta_data[0].get_data_type(attributes_to_join_on[0])
    data_type_2 = table_meta_data[0].get_data_type(attributes_to_join_on[1])
    # Standardmäßige, sinnvoll erscheinende Typkonversion, die durchgeführt wird, wenn keine Umwandlung erzwungen werden soll
    if cast_direction == 0:
        # Wenn das Join-Attribut der ersten Tabelle vom Typ Boolean ist ...
        if data_type_group_1 == 'boolean':
            # ... und jenes der zweiten Tabelle eine Zahl oder textbasiert, ...
            if data_type_group_2 == 'integer' or data_type_group_2 == 'decimal' or data_type_group_2 == 'text':
                # ... wird das Attribut der ersten Tabelle in den Datentyp des Attributs der zweiten Tabelle konvertiert.
                cast_direction = 1
        # Wenn das Join-Attribut der ersten Tabelle eine ganze Zahl ist ...
        elif data_type_group_1 == 'integer':
            # ... und jenes der zweiten Tabelle vom Typ Boolean, ...
            if data_type_group_2 == 'boolean':
                # ... wird das Attribut der zweiten Tabelle in eine ganze Zahl konvertiert.
                cast_direction = 2
            # Falls das Join-Attribut der zweiten Tabelle textbasiert ist, ...
            elif data_type_group_2 == 'text':
                # ... wird das Attribut der ersten Tabelle in Text konvertiert.
                cast_direction = 1
        # Kommazahlen im ersten Join-Attribut ...
        elif data_type_group_1 == 'decimal':
            # ... werden nur bei textbasierten Join-Attributen der zweiten Tabelle in Text konvertiert.
            if data_type_group_2 == 'text':
                cast_direction = 1
        # Ist der Datentyp des ersten Join-Attributs textbasiert, ...
        elif data_type_group_1 == 'text':
            # ... wird das Join-Attribut der zweiten Tabelle unabhängig von seinem Datentyp ebenfalls in Text konvertiert.
            cast_direction = 2
        # Ist das Join-Attribut der ersten Tabelle ein Datum, ...
        elif data_type_group_1 == 'date':
            # ... wird es nur konvertiert, wenn das Join-Attribut der zweiten Tabelle textbasiert ist.
            if data_type_group_2 == 'text':
                cast_direction = 1,
    
    # Wenn eine Typkonversion für das Join-Attribut der ersten Tabelle erzwungen werden soll oder cast_direction in der obigen Überprüfung auf 
    # 1 gesetzt wurde, ...
    if cast_direction == 1:
        # ... wird das Join-Attribut der ersten Tabelle in der Anfrage in den Datentyp des zweiten Join-Attributs konvertiert.
        join_attribute_1 = f'CAST ({join_attribute_1} AS {data_type_2})'
    # Wurde cast_direction auf 2 gesetzt, gilt entsprechend der umgekehrte Fall.
    elif cast_direction == 2:
        join_attribute_2 = f'CAST ({join_attribute_2} AS {data_type_1})'

    ### Ausführung des Joins ###
    # Erstellung der Anfrage für einen Full Outer Join, falls gewünscht 
    if full_outer_join:
        if dialect_1 == 'mariadb':
            # Für MariaDB wird hierfür die Vereinigungsmenge zwischen einem Left Outer Join und einem Right Outer Join jeweils auf den beiden Join-Attributen gebildet.
            join_query = f'{join_query} FROM {table_1} LEFT OUTER JOIN {table_2} ON {join_attribute_1} = {join_attribute_2} UNION ({join_query} FROM {table_1} RIGHT OUTER JOIN {table_2} ON {join_attribute_1} = {join_attribute_2})'
        elif dialect_1 == 'postgresql':
            # Für PostgreSQL kann das Schlüsselwort 'FULL OUTER JOIN' genutzt werden.
            join_query = f'{join_query} FROM {table_1} FULL OUTER JOIN {table_2} ON {join_attribute_1} = {join_attribute_2}'    
    else:
        # Wenn ein Inner Join gewünscht ist, kann für MariaDB und PostgreSQL dieselbe Anfrage verwendet werden.
        join_query = f'{join_query} FROM {table_1} INNER JOIN {table_2} ON {join_attribute_1} = {join_attribute_2}'
    # Ausführen des Joins
    joined_table_result = convert_result_to_list_of_lists(execute_sql_query(engine_1, text(join_query), raise_exceptions = True))

    ### Ermittlung der Attributnamen für die Anzeige ###
    # Für die Vergleichsfunktion ist es sinnvoll, den Spaltennamen die Tabellennamen voranzustellen, um sie bei gleichen Namen richtig zuordnen zu können.
    if add_table_names_to_column_names:
        column_names_for_display = [table_1 + '.' + col_1 for col_1 in attributes_to_select_1] + [table_2 + '.' + col_2 for col_2 in attributes_to_select_2]
    else:
        # Anderenfalls (d. h. für die Verbindungsfunktion) werden die Auswahlattribute lediglich zu einer Liste zusammengefügt.
        column_names_for_display = attributes_to_select_1 + attributes_to_select_2

    ### Ermittlung der Anzahl der Tupel, denen keines aus der anderen Tabelle zugeordnet werden kann ###
    unmatched_rows_query = ''
    if dialect_1 == 'mariadb':
        # Für MariaDB erfordert dies zwei Unterabfragen, damit die Zählung anhand zweier Kriterien erfolgen kann.
        unmatched_rows_query = f'SELECT (SELECT COUNT(*) FROM ({table_1} LEFT OUTER JOIN {table_2} ON {join_attribute_1} = {join_attribute_2}) WHERE {join_attribute_2} IS NULL), (SELECT COUNT(*) FROM ({table_1} RIGHT JOIN {table_2} ON {join_attribute_1} = {join_attribute_2}) WHERE {join_attribute_1} IS NULL);'
    elif dialect_1 == 'postgresql':
        # Für PostgreSQL kann dies mithilfe der Funktion FILTER innerhalb eines Full Outer Join erfolgen.
        # Als Filterattribut werden die Primärschlüssel verwendet, da diese in PostgreSQL-Datenbanken nicht NULL sein dürfen.
        filter_count_table_1 = f'{table_2}.{convert_string_if_contains_capitals_or_spaces(table_meta_data[1].primary_keys[0], dialect_2)}'
        filter_count_table_2 = f'{table_1}.{convert_string_if_contains_capitals_or_spaces(table_meta_data[0].primary_keys[0], dialect_1)}'
        unmatched_rows_query = f'SELECT COUNT(*) FILTER (WHERE {filter_count_table_1} IS NULL), COUNT(*) FILTER (WHERE {filter_count_table_2} IS NULL) FROM {table_1} FULL OUTER JOIN {table_2} ON {join_attribute_1} = {join_attribute_2}'
    # Ausführen der Abfrage und Listenumwandlung des Ergebnisses, das nur aus einem Tupel besteht   
    no_of_unmatched_rows = list(execute_sql_query(engine_1, text(unmatched_rows_query)).fetchone())
    
    
    ### Ausgabe des Ergebnisses ###
    # Falls der Parameter cast_direction für nachfolgende Funktionen (merge_two_tables) gebraucht wird, ...
    if return_cast_direction:
        # ... gebe die Ergebnistabelle, die Spaltennamen, die Liste der Zähler mit nicht zugeordneten Tupeln beider Tabellen und den aktuellen Wert von cast_direction zurück.
        return joined_table_result, column_names_for_display, no_of_unmatched_rows, cast_direction
    else:
        # Anderenfalls gebe nur die Ergebnistabelle, die Spaltennamen und die Liste der Zähler mit nicht zugeordneten Tupeln beider Tabellen zurück.
        return joined_table_result, column_names_for_display, no_of_unmatched_rows
    
def join_tables_of_different_dialects_dbs_or_servers(table_meta_data:list[TableMetaData], attributes_to_join_on:list[str], attributes_to_select_1:list[str], attributes_to_select_2:list[str], cast_direction:int = None, full_outer_join:bool = False, add_table_names_to_column_names:bool = True):
    try:
        check_arguments_for_joining(table_meta_data, attributes_to_join_on, attributes_to_select_1, attributes_to_select_2, cast_direction)
    except Exception as error:
        raise error
    # Vorbereiten der Parameter für die Abfrage
    table_meta_data_1 = table_meta_data[0]
    table_meta_data_2 = table_meta_data[1]
    table_1 = table_meta_data_1.table_name
    table_2 = table_meta_data_2.table_name
    # Anlegen der Tabellennamen, der zugehörigen Engines und der auszuwählenden Attribute jeweils als Liste, um beide Tabellen innerhalb einer 
    # Schleife abfragen zu können
    tables = [table_1, table_2]
    engines = [table_meta_data_1.engine, table_meta_data_2.engine]
    attributes_to_select = [attributes_to_select_1, attributes_to_select_2]
    # Anlegen einer Liste, in der die Ergebnisse der Abfrage als Liste gespeichert werden
    results = []
    # Anlegen einer Liste, in der die Spaltennamen der Abfrageergebnisse jeweils als Liste gespeichert werden, um sie korrekt in der Web-App
    # anzeigen zu können
    result_columns = []
    # Anlegen einer Liste zur Speicherung der Join-Attribute mit ggf. nötigen Anführungszeichen
    join_attributes = []
    # Anlegen einer Liste mit einem Boolean-Wert je Tabelle, der wiedergibt, ob das Join-Attribut der Auswahl hinzugefügt werden musste oder nicht
    join_attribute_added = [False, False]
    for index, engine in enumerate(engines):
        # Speichern des jeweiligen Join-Attributs mit Escaping
        join_attributes.append(convert_string_if_contains_capitals_or_spaces(attributes_to_join_on[index], engine.dialect.name))
        # Auflistung der auszuwählenden Attribute für die Abfrage
        selection = ''
        for attribute in attributes_to_select[index]:
            if selection == '':
                selection = f'{convert_string_if_contains_capitals_or_spaces(attribute, engine.dialect.name)}'
            else:
                selection = f'{selection}, {convert_string_if_contains_capitals_or_spaces(attribute, engine.dialect.name)}'
        # Wenn sich das Join-Attribut der jeweiligen Tabelle nicht in der zugehörigen Liste der auszuwählenden Spalten befindet ...
        if join_attributes[index] not in attributes_to_select[index]:
            # ... wird es hinten angehängt, da es für die manuelle Ausführung des Joins benötigt wird, ...
            selection = f'{selection}, {convert_string_if_contains_capitals_or_spaces(join_attributes[index], engine.dialect.name)}'
            # ... und der Boolean-Wert für diese Tabelle in join_attribute_added wird auf True gesetzt.
            join_attribute_added[index] = True
        # Erstellen der Datenbankabfrage ...
        query = f'SELECT {selection} FROM {convert_string_if_contains_capitals_or_spaces(tables[index], engine.dialect.name)}'
        # ... und Ausführung mit Ausgabe von Fehlermeldungen, die auf dem Server abgefangen werden.
        result = execute_sql_query(engine, text(query), raise_exceptions = True)
        # Kopieren der Spaltennamen des Abfrageergebnisses als Liste ...
        result_columns.append(list(result.keys()))
        # ... und Eintragen des Abfrageergebnisses als Liste von Listen in das Dictionary results ein, damit mehrfach über die Ergebniszeilen iteriert
        # werden kann.
        results.append(convert_result_to_list_of_lists(result))

    # Auslesen der Datentypgruppen der Join-Attribute ...
    data_type_group_1 = table_meta_data_1.get_data_type_group(attributes_to_join_on[0])
    data_type_group_2 = table_meta_data_2.get_data_type_group(attributes_to_join_on[1])
    # ... und ihrer Position in der Liste der Spaltennamen des Abfrageergebnisses
    join_attribute_index_1 = result_columns[0].index(attributes_to_join_on[0])
    join_attribute_index_2 = result_columns[1].index(attributes_to_join_on[1])
    # Anlegen einer Liste zur Speicherung der verbundenen Tabelle
    joined_table = []
    # Anlegen einer Liste mit Übereinstimmungszählern für jedes Tupel in Tabelle 2; für die Erstellung eines Full Outer Joins
    match_counter_table_2 = [0] * len(results[1])
    # Anlegen einer Liste mit der Anzahl Tupel ohne Übereinstimmung in der anderen Tabelle; für die Statistik nicht zugeordneter Tupel und zur
    # Einschätzung, ob ein Join eindeutig ist
    no_of_unmatched_rows = [0, 0]
    # Iteriere die Tupel der ersten Tabelle durch ...
    for row_1_index, row_1 in enumerate(results[0]):
        # ... und setze ihren Übereinstimmungszähler auf 0.
        match_counter_row_1 = 0
        # Beginne die Überprüfung der Übereinstimmung des aktuellen Tupels der ersten Tabelle mit den einzelnen Tupeln der zweiten Tabelle.
        for row_2_index, row_2 in enumerate(results[1]):
            # Boolean-Wert, der wiedergibt, ob die beiden aktuellen Tupel überstimmen oder nicht; hiervon hängt ab, ob die Tupel in die zurück-
            # gegebene Ergebnistabelle aufgenommen werden oder nicht.
            is_match = False
            # Gemäß den üblichen SQL-Regeln werden NULL-Werte (bzw. None in Python) aus der Übereinstimmungsüberprüfung ausgeschlossen.
            if row_1[join_attribute_index_1] is not None and row_2[join_attribute_index_2] is not None:
                # Stimmen die Join-Attribute direkt (d. h. ohne Typkonversion) überein, ...
                if row_1[join_attribute_index_1] == row_2[join_attribute_index_2]:
                    # ... setze den Boolean-Wert hierfür auf True.
                    is_match = True
                # Falls in der Web-App ausgewählt wurde, dass für eines der beiden Join-Attribute Typkonversionen erzwungen werden sollen, ...
                elif cast_direction == 1 or cast_direction == 2:
                    # ... führe die erzwungene Konversion und die Übereinstimmungsüberprüfung in der Hilfsfunktion force_cast_and_match durch.
                    cast_result = force_cast_and_match(data_type_group_1, data_type_group_2, [row_1[join_attribute_index_1], row_2[join_attribute_index_2]], cast_direction)
                    # Gibt diese ein Tupel zurück, liegt eine Übereinstimmung vor.
                    if type(cast_result) == tuple:
                        # Der Boolean-Wert hierfür steht im ersten Wert des Tupels, ..
                        is_match = cast_result[0]
                        # ... der konvertierte Wert im zweiten. Ordne diesen der vorgegebenen Konversionsrichtung entsprechend ...
                        if cast_direction == 1:
                            # ... dem Join-Attribut in Tabelle 1 ...
                            row_1[join_attribute_index_1] = cast_result[1]
                        elif cast_direction == 2:
                            # ... oder Tabelle 2 zu.
                            row_2[join_attribute_index_2] = cast_result[1]
                    # Gibt force_cast_and_match kein Tupel (d. h. nur einen Wert) zurück, ...
                    else:
                        # ... liegt keine Übereinstimmung vor.
                        is_match = False
                ### Beginn der standardmäßigen Übereinstimmungsüberprüfung mit impliziten Typkonversionen ###
                # Falls das Join-Attribut der ersten Tabelle ein Boolean-Wert ist ...
                elif data_type_group_1 == 'boolean':
                    # ... und jenes der zweiten Tabelle eine ganze Zahl, ...
                    if data_type_group_2 == 'integer':
                        try:
                            # ... überprüfe, ob der in eine ganze Zahl konvertierte Boolean-Wert mit dem Wert aus der zweiten Tabelle übereinstimmt.
                            if int(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                                # Trifft dies zu, speichere die Übereinstimmung in is_match.
                                is_match = True
                        # Tritt bei der Konversion hingegen ein Fehler auf, ...
                        except ValueError:
                            # ... liegt keine Übereinstimmung vor.
                            is_match = False
                    # Dieses Prinzip wird nachfolgend jeweils wiederholt.
                    # Bei der Kombination Boolean in der ersten, Dezimalzahl in der zweiten Tabelle ...
                    elif data_type_group_2 == 'decimal':
                        try:
                            # ... wird versucht, den Boolean-Wert in eine Dezimalzahl zu konvertieren.
                            if float(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                                is_match = True
                        except ValueError:
                            is_match = False
                    # Ist das Attribut der zweiten Tabelle textbasiert, ...
                    elif data_type_group_2 == 'text':
                        try:
                            # ... wird der Boolean-Wert in Text zu konvertieren versucht.
                            if str(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                                is_match = True
                        except ValueError:
                            is_match = False
                # Ist das Join-Attribut aus der ersten Tabelle hingegen eine ganze Zahl ...
                elif data_type_group_1 == 'integer':
                    # ... und jenes aus der zweiten Tabelle ein Boolean-Wert, ...
                    if data_type_group_2 == 'boolean':
                        try:
                            # ... wird der Wert aus der zweiten Tabelle zu konvertieren versucht.
                            if row_1[join_attribute_index_1] == int(row_2[join_attribute_index_2]):
                                is_match = True
                        except ValueError:
                            is_match = False
                    # Ist das Join-Attribut der zweiten Tabelle textbasiert, ...
                    elif data_type_group_2 == 'text':
                        try:
                            # ... wird versucht, den Wert aus der ersten Tabelle in einen String zu konvertieren.
                            if str(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                                is_match = True
                        except ValueError:
                            is_match = False
                # Ist das Join-Attribut der ersten Tabelle eine Dezimalzahl ...
                elif data_type_group_1 == 'decimal':
                    # ... und jenes der zweiten Tabelle ein Boolean-Wert
                    if data_type_group_2 == 'boolean':
                        try:
                            # ... wird der Wert aus der zweiten Tabelle in eine Dezimalzahl zu konvertieren versucht.
                            if row_1[join_attribute_index_1] == float(row_2[join_attribute_index_2]):
                                is_match = True
                        except ValueError:
                            is_match = False
                    # Ist das Join-Attribut der zweiten Tabelle textbasiert, ...
                    elif data_type_group_2 == 'text':
                        try:
                            # ... wird versucht, den Wert aus der ersten Tabelle in einen String zu konvertieren.
                            if str(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                                is_match = True
                        except ValueError:
                            is_match = False
                # Ist das Join-Attribut der ersten Tabelle textbasiert, ...
                elif data_type_group_1 == 'text':
                    try:
                        # ... wird unabhängig von seinem Datentyp versucht, das Join-Attribut der zweiten Tabelle in einen String zu konvertieren.
                        if row_1[join_attribute_index_1] == str(row_2[join_attribute_index_2]):
                            is_match = True
                    except ValueError:
                        is_match = False
                # Für Join-Attribute der ersten Tabelle mit einem Datumsdatentyp ...
                elif data_type_group_1 == 'date':
                    # ... erfolgt die Überprüfung nur, wenn das Join-Attribut aus der zweiten Tabelle einen textbasierten Datentyp aufweist.
                    if data_type_group_2 == 'text':
                        try:
                            if str(row_1[join_attribute_index_1]) == row_2[join_attribute_index_2]:
                                is_match = True
                        except ValueError:
                            is_match = False
            # Erstelle eine Kopie des aktuellen Tupels der ersten Tabelle, damit diese bearbeitet werden kann, ohne das Ergebnis der nächsten Iteration
            # zu verfälschen.
            row_1_copy = row_1.copy()
            # Wurde das Join-Attribut der ersten Tabelle der Attributsauswahl zur Ermöglichung des Joins hinzugefügt, ...
            if join_attribute_added[0] == True:
                # ... entferne es aus der Kopie des Tupels der ersten Tabelle.
                row_1_copy.pop(join_attribute_index_1)
            # Erstelle eine Kopie des aktuellen Tupels der zweiten Tabelle, damit diese bearbeitet werden kann, ohne das Ergebnis der nächsten Iteration
            # zu verfälschen.
            row_2_copy = row_2.copy()
            # Wurde das Join-Attribut der zweiten Tabelle der Attributsauswahl zur Ermöglichung des Joins hinzugefügt, ...
            if join_attribute_added[1] == True:
                # ... entferne es aus der Kopie des aktuellen Tupels der zweiten Tabelle.
                row_2_copy.pop(join_attribute_index_2)
            # Wurde eine Übereinstimmung festgestellt, ...
            if is_match:
                # ... erhöhe die Zähler für beide Tabellen jeweils um den Wert 1 ...
                match_counter_row_1 += 1
                match_counter_table_2[row_2_index] += 1
                # ... und die füge die Tupel als eine Liste zusammengefügt der Ergebnistabelle hinzu.
                joined_table.append(row_1_copy + row_2_copy)
            # Liegt hingegen keine Übereinstimmung vor ...
            else:
                # ... und es handelt sich um die letzte Iteration dieses Tupels der zweiten Tabelle (d. h. das überprüfte Tupel aus der ersten 
                # Tabelle ist das letzte dort), während für das aktuelle Tupel der zweiten Tabelle keine Übereinstimmung gefunden wurde, ...
                if row_1_index == len(results[0]) - 1 and match_counter_table_2[row_2_index] == 0:
                    # ... erhöhe den Zähler für nicht zuzuordnende Tupel der zweiten Tabelle um den Wert 1.
                    no_of_unmatched_rows[1] += 1 
                    # War zudem ein Full Outer Join gewünscht, ...
                    if full_outer_join:
                        # hänge der Ergebnistabelle eine Zeile an, in der die Attribute der ersten Tabelle jeweils leer sind und in die nur die
                        # Werte des Tupels aus der zweiten Tabelle übernommen wurden. 
                        joined_table.append(([None] * len(row_1_copy) + row_2_copy))  
        # Gab es keine Übereinstimmung des aktuellen Tupels aus der ersten Tabelle mit einem Tupel der zweiten Tabelle, ...
        if match_counter_row_1 == 0:
            # ... erhöhe den Zähler für nicht zuzuordnende Tupel aus der ersten Tabelle um den Wert 1.
            no_of_unmatched_rows[0] += 1
            # War zudem ein Full Outer Join gewünscht, ...
            if full_outer_join:
                # ... hänge der Ergebnistabelle eine Zeile an, in der die Werte des aktuellen Tupels der ersten Tabelle stehen und die Attribute
                # der zweiten Tabelle jeweils leer sind.
                joined_table.append(row_1 + [None] * len(row_2_copy))

    # Wenn das Join-Attribut der Auswahl der Attribute für eine der beiden Tabellen zur Ermöglichung des Joins hinzugefügt wurde, aber in der
    # Web-App nicht angezeigt werden soll, ...
    if join_attribute_added[0] == True:
        # ... entferne es aus der Liste der Spaltennamen der Ergebnistabelle.
        result_columns[0].pop(join_attribute_index_1)
    if join_attribute_added[1] == True:
        result_columns[1].pop(join_attribute_index_2)
    # Wenn gewünscht, füge allen Spaltennamen des Ergebnisses den zugehörigen Tabellennamen hinzu, um sie in der Web-App bei gleichen Spaltennamen besser identifizieren
    # zu können. Füge die beiden Liste außerdem zu einer zusammen.
    if add_table_names_to_column_names == True:
        column_names_for_display = [table_1 + '.' + col_1 for col_1 in result_columns[0]] + [table_2 + '.' + col_2 for col_2 in result_columns[1]]
    # Anderenfalls (für Merge) füge die Spaltennamen nur zu einer Liste zusammen.
    else:
        column_names_for_display = result_columns[0] + result_columns[1]
    # Falls der Parameter für nachfolgende Funktionen (merge_two_tables) gebraucht wird, ...
    # Gebe die Ergebnistabelle, die Spaltennamen und die Liste der Zähler mit nicht zugeordneten Tupeln beider Tabellen zurück.
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

"""Führt einen Join zweier Tabellen über zwei Attribute aus und fügt die Werte eines ausgewählten Attributes in eine bestehende oder neu erstellte Spalte der Zieltabelle ein.

Gibt einen ArgumentError aus, wenn die Parameter ungeeignet sind oder die Attributnamen nicht korrekt zugeordnet werden können."""
def simulate_merge_and_build_query(target_table_data:TableMetaData, source_table_data:TableMetaData, attributes_to_join_on:list[str], source_attribute_to_insert:str, target_column:str|None, cast_direction:int, new_attribute_name:str|None, add_table_names_to_column_names:bool = True):
    ### Überprüfung, ob die übergebenen Argumente die erwartete Form und die erwarteten Inhalte haben ###
    if type(target_table_data) != TableMetaData or type(source_table_data) != TableMetaData:
        raise ArgumentError(None, 'Die Parameter target_table_data und source_table_data müssen vom Typ TableMetaData sein.')
    if source_attribute_to_insert not in source_table_data.columns:
        raise ArgumentError(None, 'Die zu übernehmende Spalte muss zur Quelltabelle gehören.')
    if target_column is not None and target_column not in target_table_data.columns:
        raise ArgumentError(None, 'Die Zielspalte muss zur Zieltabelle gehören.')
    if cast_direction not in (0, 1, 2):
        raise ArgumentError(None, 'Der Parameter cast_direction darf nur die Werte 0, 1 oder 2 annehmen.')
    if new_attribute_name is not None and new_attribute_name in target_table_data.columns:
        raise ArgumentError(None, 'In der Zieltabelle darf keine Spalten mit dem neuen Spaltennamen existieren.')
    if new_attribute_name is None and target_column is None and source_attribute_to_insert in target_table_data.columns:
        raise ArgumentError(None, 'Wenn eine neue Spalte angelegt werden soll und hierfür kein neuer Name angegeben ist, darf die Zieltabelle keine Spalte mit dem Namen des Quellattributs haben.')
    if target_column is not None and (new_attribute_name is not None and new_attribute_name.strip() != ''):
        raise ArgumentError(None, 'Wenn eine existierende Spalte als Ziel der Operation angegeben ist, kann hierfür kein neuer Name gewählt werden.')
    if attributes_to_join_on[0] not in target_table_data.columns or attributes_to_join_on[1] not in source_table_data.columns:
        raise ArgumentError(None, 'Das erste Attribut in attributes_to_join_on muss zur Zieltabelle gehören, das zweite zur Quelltabelle.')
    
    ### Anlegen von Variablen zum Speichern der benötigten Teile der Tabellenmetadaten für leichteren Zugriff ###
    target_engine = target_table_data.engine
    source_engine = source_table_data.engine
    target_dialect = target_engine.dialect.name
    source_dialect = source_engine.dialect.name
    target_table = target_table_data.table_name
    target_attributes_to_select = target_table_data.columns
    
    ### Überprüfung, ob über die angegebenen Join-Attribute ein Join mit eindeutiger Zuordnung möglich ist ###
    # Wenn beide Tabellen in derselben Datenbank auf demselben Server oder im Fall von MariaDB zwar in verschiedenen Datenbanken, aber auf demselben Server liegen, ...
    if target_engine.url == source_engine.url or (target_dialect == 'mariadb' and source_dialect == 'mariadb' and target_engine.url.host == source_engine.url.host and target_engine.url.port == source_engine.url.port):
        # ... führe die Funktion für den Join zweier Tabellen auf demselben Server aus. Da der Parameter cast_direction hierbei ggf. geändert wird, wird er hier mit ausgegeben.
        joined_result, joined_column_names, unmatched_rows, cast_direction = join_tables_of_same_dialect_on_same_server([target_table_data, source_table_data], attributes_to_join_on, target_attributes_to_select, [source_attribute_to_insert], cast_direction, False, add_table_names_to_column_names, return_cast_direction = True)
    # In allen anderen Fällen ...
    else:
        # ... wähle die Variante mit dem nicht SQL-basierten Join.
        joined_result, joined_column_names, unmatched_rows = join_tables_of_different_dialects_dbs_or_servers([target_table_data, source_table_data], attributes_to_join_on, target_attributes_to_select, [source_attribute_to_insert], cast_direction, False, add_table_names_to_column_names)
    # Wenn die Summe der Anzahl der Tupel im Ergebnis und der nicht zuzuordnenden Tupel nicht der Gesamtanzahl der Tupel der Zieltabelle entspricht, kann keine eindeutige Zuordnung der neuen Werte erfolgen. 
    if len(joined_result) + unmatched_rows[0] != target_table_data.total_row_count:
        # Daher wird der Prozess abgebrochen und eine Fehlermeldung ausgegeben.
        raise MergeError('Mindestens einem Tupel der Zieltabelle konnte mehr als ein Tupel aus der Quelltabelle zugeordnet werden. Bitte wählen Sie Join-Attribute mit eindeutigen Werten.')
    
    ### Überprüfung der Kompatibilität des Zielattributs und des Quellattributs ###
    # Anlegen der benötigten Variablen für erleichterten Zugriff
    source_data_type_info = source_table_data.data_type_info[source_attribute_to_insert]
    source_data_type_group = source_table_data.get_data_type_group(source_attribute_to_insert)
    source_data_type = source_table_data.get_data_type(source_attribute_to_insert)
    target_data_type_info = None
    target_data_type_group = None
    target_data_type = None
    add_column_query = ''
    # Wenn ein neues Attribut in die Zieltabelle eingefügt werden soll und hierfür kein neuer Name angegeben ist, ...
    if new_attribute_name is None or new_attribute_name == '':
        # ... wird der Name des Quellattributs übernommen.
        new_attribute_name = source_attribute_to_insert
    # Für die Abfragen wird der Name für die neue Spalte als ggf. mit Anführungszeichen umgebene Kopie gespeichert.
    escaped_new_column_name = convert_string_if_contains_capitals_or_spaces(new_attribute_name, target_engine)
    # Wenn ein bestehendes Attribut der Zieltabelle zum Eintragen der Werte aus der Quelltabelle ausgewählt wurde, ...
    if target_column is not None:
        # ... beziehe die Metadaten dieses Attributs. 
        target_data_type_info = target_table_data.data_type_info[target_column]
        target_data_type_group = target_table_data.get_data_type_group(target_column)
        target_data_type = target_table_data.get_data_type(target_column)
        # Das Zielattribut für die Update-Anweisung wurde in diesem Fall direkt als Parameter angegeben.
        target_attribute = convert_string_if_contains_capitals_or_spaces(target_column, target_dialect)
        # Ausgabe von Fehlermeldungen, ...
        # ... wenn die Datentypgruppen der beiden Attribute nicht übereinstimmen (z. B. wenn eines textbasiert ist, das andere jedoch eine Zahl).
        if not target_data_type_group == source_data_type_group:
            raise MergeError(f'Die Datentypen {target_data_type} und {source_data_type} der Ziel- und der Quelltabelle sind nicht kompatibel.')
        # ... wenn bei textbasierten Datentypen die maximal erlaubte Zeichenanzahl des Zielattributs unter jener des Quellattributs liegt.
        elif target_data_type_group == 'text' and target_data_type_info['character_max_length'] < source_data_type_info['character_max_length']:
            raise MergeError(f'Die maximal erlaubte Zeichenanzahl in {target_column} reicht eventuell nicht aus, um alle Einträge des Attributs {source_attribute_to_insert} zu speichern.')
        # ... wenn es sich bei beiden Datentypen um ganze Zahlen handelt, ...
        elif target_data_type_group == 'integer':
            # ... der Wertebereich des Zielattributs jedoch kleiner ist als jener des Quellattributs.
            if (target_data_type == 'tinyint' and source_data_type != 'tinyint') or (target_data_type == 'smallint' and source_data_type not in ('smallint', 'tinyint')) or (target_data_type == 'mediumint' and source_data_type == 'bigint') or ((target_dialect == 'postgresql' and target_data_type != 'numeric') and (source_dialect == 'mariadb' and source_data_type == 'bigint' and 'is_unsigned' in source_data_type_info.keys())):
                raise MergeError(f'Der Wertebereich des Zieldatentyps {target_data_type} reicht eventuell nicht aus, um alle Einträge des Attributs {source_attribute_to_insert} zu speichern.')
    # Wenn kein bestehendes Attribut der Zieltabelle zum Eintragen der neuen Werte ausgewählt wurde ...
    else:
        # ... und sich die SQL-Dialekte der beiden Datenbanken unterscheiden, erfolgt die Kompatibilitätsprüfung mithilfe der statischen Kompatibilitäts-Dictionarys in CompatibilityClasses.
        if target_dialect != source_dialect:
            if source_dialect == 'mariadb' and target_dialect == 'postgresql':
                # Für MariaDB ist der Sonderfall der vorzeichenlosen ganzen Zahlen zu beachten, in dem der sonst für negative Zahlen reservierte Bereich auch für positive Zahlen verwendet wird, sodass sich deren Wertebereich verdoppelt.
                if source_data_type_group == 'integer' and 'is_unsigned' in source_data_type_info.keys():
                    source_data_type = f'{source_data_type} unsigned'
                # Wenn der Quelldatentyp nicht im Kompatibilitäs-Dictionary enthalten ist, ...
                if source_data_type not in MariaToPostgresCompatibility.data_types.keys():
                    # ... wird dies als mangelnde Kompatibilität bewertet und eine Fehlermeldung ausgegeben.
                    raise MergeError(f'Die Datentypen {target_data_type} und {source_data_type} der Ziel- und der Quelltabelle sind nicht kompatibel.')
                # Ist der Quelldatentyp als Schlüssel im Kompatibilitäts-Dictionary enthalten, ...
                else:
                    # ... wird der entsprechende Wert als Datentyp für das neu einzufügende Attribut der Zieltabelle übernommen.
                    target_data_type = MariaToPostgresCompatibility.data_types[source_data_type]
            # Die umgekehrte Richtung erfolgt analog.
            elif source_dialect == 'postgresql' and target_dialect == 'mariadb':
                if source_data_type not in PostgresToMariaCompatibility.data_types.keys():
                    raise MergeError(f'Die Datentypen {target_data_type} und {source_data_type} der Ziel- und der Quelltabelle sind nicht kompatibel.')
                else:
                    target_data_type = PostgresToMariaCompatibility.data_types[source_data_type]
            # Lege ein Dictionary für die Metadaten des Zielattributdatentyps an, ...
            target_data_type_info = {}
            # ... übernehme die Datentypgruppe des Quellattributs, da diese übereinstimmen muss, ...
            target_data_type_info['data_type_group'] = source_data_type_group
            # ... und trage als Datentyp das aus dem Kompatibilitäts-Dictionary ermittelte Äquivalent der Zieltabelle ein.
            target_data_type_info['data_type'] = target_data_type
            # Alle Einträge zu den Attributsmetadaten, ...
            for key in source_data_type_info.keys():
                # ... die sich nicht auf die Bezeichnung des Datentyps bzw. dessen Gruppe beziehen, ...
                if 'data_type' not in key:
                    # ... werden vom Quellattribut übernommen.
                    target_data_type_info[key] = source_data_type_info[key]
        # Unterscheiden sich die Dialekte der Datenbanken bzw. Tabellen hingegen nicht (z. B. bei zwei PostgreSQL-Datenbanken auf demselben Server), ...
        else:
            # ... entspricht der Datentyp des Zielattributs exakt jenem des Quellattributs.
            target_data_type_info = source_data_type_info
        
        # Erstellen der Abfrage zum Einfügen des neuen Attributs mithilfe der zuvor gewonnenen Daten
        add_column_query = build_query_to_add_column(target_table_data, escaped_new_column_name, target_data_type_info)
        # Das Zielattribut für die Update-Anweisung entspricht in diesem Fall dem Namen für die neu anzulegende Spalte.
        target_attribute = escaped_new_column_name
    
    ### Erstellen der SQL-Abfrage für die Aktualisierung der Zieltabelle ###
    # Anlegen der Variablen mit den Informationen für die Update-Anweisung für bessere Lesbarkeit
    source_db = convert_string_if_contains_capitals_or_spaces(source_engine.url.database, source_dialect)
    source_table = convert_string_if_contains_capitals_or_spaces(source_table_data.table_name, source_dialect)
    source_attribute = convert_string_if_contains_capitals_or_spaces(source_attribute_to_insert, source_dialect)
    source_join_attribute = convert_string_if_contains_capitals_or_spaces(attributes_to_join_on[1], source_dialect)
    source_join_attribute_data_type = source_table_data.get_data_type(attributes_to_join_on[1])

    target_db = convert_string_if_contains_capitals_or_spaces(target_engine.url.database, target_dialect)
    target_table = convert_string_if_contains_capitals_or_spaces(target_table, target_dialect)
    target_join_attribute = convert_string_if_contains_capitals_or_spaces(attributes_to_join_on[0], target_dialect)
    target_join_attribute_data_type = target_table_data.get_data_type(attributes_to_join_on[0])

    # Anlegen des Parameter-Dictionarys für die UPDATE-Anweisung
    params = {}
    
    update_query = None
    ### Für PostgreSQL-Tabellen derselben Datenbank ###
    if source_engine.url == target_engine.url and target_dialect == 'postgresql':
        # Typkonversion für das Join-Attribut der Zieltabelle, wenn dies in der Web-App ausgewählt wurde
        if cast_direction == 1:
            join_condition = f'WHERE CAST({target_table}.{attributes_to_join_on[0]} AS {source_join_attribute_data_type}) = {source_table}.{attributes_to_join_on[1]}'
        # Typkonversion für das Join-Attribut der Quelltabelle, wenn dies in der Web-App ausgewählt wurde
        elif cast_direction == 2:
            join_condition = f'WHERE {target_table}.{attributes_to_join_on[0]} = CAST({source_table}.{attributes_to_join_on[1]} AS {target_join_attribute_data_type})'
        # Anderenfalls (d. h. wenn cast_direction den Wert 0 hat), ...
        else:
            # ... werden die Attribute ohne Typkonversion gleichgesetzt.
            join_condition = f'WHERE {target_table}.{attributes_to_join_on[0]} = {source_table}.{attributes_to_join_on[1]}'
        
        # Erstellen der Update-Anweisung
        update_query = f'UPDATE {target_table} SET {target_attribute} = {source_table}.{source_attribute} FROM {source_table} {join_condition}'
        # alternativ:
        # update_query = f'MERGE INTO {target_table} t_tbl USING {source_table} s_tbl ON t_tbl.{target_join_attribute} = s_tbl.{source_join_attribute} WHEN MATCHED THEN UPDATE SET {target_attribute} = {source_attribute} WHEN NOT MATCHED THEN DO NOTHING'  
    ### Für MariaDB-Tabellen in Datenbanken auf demselben Server ###
    elif (target_dialect == 'mariadb' and source_dialect == 'mariadb') and (target_engine.url.host == source_engine.url.host) and (target_engine.url.port == source_engine.url.port):
        # Falls die Quell- und die Zieltabelle in unterschiedlichen Datenbanken liegen, können sich bei Tabellen mit dem gleichen Namen Konflikte ergeben.
        if source_db != target_db:
            # Um dies zu verhindern, wird den Tabellennamen jeweils der Datenbankname gemäß der SQL-Syntax durch einen Punkt abgetrennt vorangestellt.
            source_table = f'{source_db}.{source_table}'
            target_table = f'{target_db}.{target_table}'
        # Um Daten aus einer anderen Tabelle in die Zieltabelle eintragen zu können, wird in diesem Fall ein Inner Join mit einer weiteren Datenbankabfrage benötigt, deren Ergebnisse hier als "sub_tabl" bezeichnet werden.
        join_condition = f'{target_table}.{target_join_attribute} = sub_tbl.{source_join_attribute}'
        # Falls das Attribut der Zieltabelle ...
        if cast_direction == 1:
            join_condition = f'CAST({target_table}.{target_join_attribute} AS {source_join_attribute_data_type}) = sub_tbl.{source_join_attribute}'
        # ... oder der Quelltabelle einer Typkonversion unterzogen werden soll, ...
        elif cast_direction == 2:
            # ... wird die Join-Bedingung entsprechend angepasst.
            join_condition = f'{target_table}.{target_join_attribute} = CAST(sub_tbl.{source_join_attribute} AS {target_join_attribute_data_type})'
        # Zusammenfügen der Update-Anweisung (Prinzip entnommen aus https://stackoverflow.com/questions/51977955/update-mariadb-table-using-a-select-query)
        update_query = f'UPDATE {target_table} INNER JOIN (SELECT {source_join_attribute}, {source_attribute} FROM {source_table}) sub_tbl ON {join_condition} SET {target_table}.{target_attribute} = sub_tbl.{source_attribute}'
    # Falls die Datenbanken auf unterschiedlichen Servern liegen, unterschiedliche SQL-Dialekte haben oder (im Fall von PostgreSQL) in verschiedenen Datenbanken des gleichen Dialekts liegen, kann der Join nicht SQL-basiert erfolgen.
    else:
        # Daher werden zur Identifizierung der Tupel deren Primärschlüsselwerte benötigt. Dazu sind die ggf. mit Anführungszeichen versehenen Namen der Primärschlüsselattribute ...
        escaped_pk_columns = []
        # ... sowie ihre Positionen im Join-Ergebnis erforderlich.
        pk_indexes = []
        # Daher wird für jedes Primärschlüsselattribut der Zieltabelle überprüft, ...
        for key in target_table_data.primary_keys:
            # ... welchem der Attribute im Join-Ergebnis es entspricht.
            for index, attribute in enumerate(joined_column_names):
                # Wurde das entsprechende Attribut gefunden, ...
                if attribute == key:
                    # ... wird die Position als Primärschlüsselattributindex gespeichert ...
                    pk_indexes.append(index)
                    # ... ebenso wie der ggf. mit Anführungszeichen versehene Attributname.
                    escaped_pk_columns.append(convert_string_if_contains_capitals_or_spaces(key, target_dialect))
        # Außerdem wird die Position des Quellattributs im Join-Ergebnis benötigt.
        source_attribute_index = joined_column_names.index(source_attribute_to_insert)
        

        target_attribute = convert_string_if_contains_capitals_or_spaces(target_attribute, target_dialect)
        target_table = convert_string_if_contains_capitals_or_spaces(target_table, target_dialect)
        # Nun wird die Update-Anweisung erstellt, mit einer Case-Anweisung für jedes Tupel des Joins.     
        update_query = f'UPDATE {target_table} SET {target_attribute} ='
        # Dabei werden die Werte gezählt, die nicht den Wert 'NULL' haben.
        not_null_counter =  0
        for row in joined_result:
            # Falls der einzutragende Wert 'None' lautet, wird dies mit der abschließenden Bedingung 'ELSE DEFAULT' abgefangen.
            if row[source_attribute_index] is None:
                # Somit kann diese Iteration der Schleife übersprungen werden.
                continue
            # Für andere Werte als 'NULL' wird eine Case-Bedingung...
            else:
                # ... begonnen, wenn es der erste Nicht-NULL-Wert ist, ...
                if not_null_counter == 0:
                    condition = 'CASE WHEN'
                # ... und anderenfalls weitergeführt.
                else:
                    condition = f'WHEN'
                # Zähler für Nicht-NULL-Werte erhöhen
                not_null_counter += 1
                # Die einzutragenden Werte werden jeweils über die Primärschlüsselwerte des Tupels identifiziert.
                for pk_index, key in enumerate(escaped_pk_columns):
                    # Falls die Tabelle mehr als ein Primärschlüsselattribut hat und es sich nicht um das erste handelt, ...
                    if pk_index != 0:
                        # ... werden die Bedingungen für Primärschlüsselwerte mit AND verknüpft.
                        condition = f'{condition} AND'
                    condition = f'{condition} {key} = {row[pk_index]}'
                params['value_' + str(not_null_counter)] = row[source_attribute_index]
                update_query = f"{update_query} {condition} THEN :{'value_' + str(not_null_counter)}"
        # Wenn mindestens einer der einzutragenden Werte nicht 'NULL' ist, ...
        if not_null_counter > 0:
            # ... wird die Case-Anweisung damit abgeschlossen, dass alle Einträge des Zielattributs ohne Wert in der Quelltabelle auf den Standardwert gesetzt werden.
            update_query = f'{update_query} ELSE DEFAULT END;'
        # Anderenfalls werden alle Werte des eingefügten Attributs auf den Standardwert gesetzt.
        else:
            update_query = f'{update_query} DEFAULT;'

    ### Ausführung der Update-Anweisung für alle SQL-Dialekt-Konstellationen ###
    result = None
    
        # Zunächst werden die (ggf. leere) Anweisung zum Hinzufügen der neuen Spalte und die Update-Anweisung zusammengefügt, um sie unmittelbar nacheinander über die gleiche Datenbankverbindung ausführen zu können.
    add_and_update_query = f'{add_column_query} {update_query}'
    print(add_and_update_query)
    text_query = text(add_and_update_query)
    with target_engine.connect() as connection:
        if params != None:
            for key in params.keys():
                text_query.bindparams(bindparam(key))
            connection.execute(text_query, params)
        # Ausführung der Update-Anweisung
        else:
            connection.execute(text_query)
        # Anschließend wird der neue Stand der Zieltabelle über dieselbe Verbindung abgefragt, um das Ergebnis auch ohne Speicherung beziehen zu können.
        result = connection.execute(text(f'SELECT * FROM {target_table}'))
        # Zusätzlich zum Ergebnis wird die Anweisung für das Erstellen des neuen Attributs und für die Aktualisierung der Zieltabelle ausgegeben, damit diese nicht neu erstellt werden muss.
        return result, add_and_update_query, params

def build_query_to_add_column(table_meta_data:TableMetaData, attribute_name:str, target_column_data_type_info:dict[str:str]):
    # Für die Erstellung der Anweisung zum Einfügen der neuen Spalte werden der Datentyp ...
    if 'data_type' not in target_column_data_type_info.keys():
        raise ArgumentError('Bitte geben Sie den Datentyp für die neue Spalte an.')
    # ... und die Datentypgruppe des Zielattributs benötigt.
    if 'data_type_group' not in target_column_data_type_info.keys():
        # Fehlt eine dieser Angaben, wird daher eine Fehlermeldung ausgegeben.
        raise ArgumentError('Bitte geben Sie die Datentypgruppe (z. B. integer, boolean, decimal, text, date) für die neue Spalte an.')
    # Anlegen der nötigen Variablen
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    data_type = target_column_data_type_info['data_type']
    data_type_group = target_column_data_type_info['data_type_group']
    # Versehen der Tabellen- und Attributnamen mit Anführungszeichen, falls nötig
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, db_dialect)
    attribute_name = convert_string_if_contains_capitals_or_spaces(attribute_name, db_dialect)
    
    # Für ganzzahlige Datentypen ...
    if data_type_group == 'integer':
        # ... ist der Wert numeric_precision angegeben. In PostgreSQL wird dieser automatisch angelegt.
        numeric_precision = target_column_data_type_info['numeric_precision']
        # In MariaDB hingegen ...
        if db_dialect == 'mariadb':
            # ... kann das Attribut direkt mit dieser Information angelegt werden, um Speicherplatz zu sparen.
            data_type = f'{data_type}({numeric_precision})'
            # Ist das Attribut zudem vorzeichenlos, ...
            if 'is_unsigned' in target_column_data_type_info.keys():
                # ... wird dies ebenfalls übernommen.
                data_type = f'{data_type} unsigned'
    # Für Dezimalzahlen ...
    elif data_type_group == 'decimal':
        # ... wird zunächst sichergestellt, dass die benötigten Parameter numeric_precision und numeric_scale als ganze Zahlen vorliegen.
        if type(target_column_data_type_info['numeric_precision']) == int and type(int(target_column_data_type_info['numeric_scale'])) == int:
            numeric_precision = target_column_data_type_info['numeric_precision']
            numeric_scale = target_column_data_type_info['numeric_scale']
            # Ist dies der Fall, wird der exakte Datentyp durch den Zusatz (numeric_precision, numeric_scale) festgelegt.
            data_type = f'{data_type}({numeric_precision, numeric_scale})'
            # Anderenfalls wird ausschließlich der Datentyp angegeben, sodass die beiden Parameter implizit dialektspezifisch festgelegt werden.
    # Für textbasierte Datentypen ...
    elif data_type_group == 'text':
        # ... wird zunächst überprüft, ob eine maximale Zeichenlänge als ganze Zahl angegeben ist.
        if 'character_max_length' in target_column_data_type_info.keys() and target_column_data_type_info['character_max_length'] != None and  type(target_column_data_type_info['character_max_length']) == int:
            # Trifft dies zu, wird sie übernommen ...
            character_max_length = target_column_data_type_info['character_max_length']
            # ... und dem Datentyp angehängt.
            data_type = f'{data_type}({character_max_length})'
            # Anderenfalls wird diese Angabe ausgelassen, sodass dieser Wert automatisch von der Datenbank festgelegt wird.
    # Für Datumsdatentypen ...
    elif data_type_group == 'date':
        # ... wird das Vorhandensein des Parameters datetime_precision als ganze Zahl überprüft.
        if 'datetime_precision' in target_column_data_type_info.keys() and target_column_data_type_info['datetime_precision'] != None and type(target_column_data_type_info['datetime_precision']) == int: 
            # Analog zu den vorigen Datentypen wird diese Angabe übernommen ...
            datetime_precision = target_column_data_type_info['datetime_precision']
            data_type = f'{data_type}({datetime_precision})'
            # ... bzw. bei Fehlen ausgelassen, sodass der Wert von der Datenbank automatisch gewählt wird.
    ### Zufügen von Unique-Constraints und Standardwerten ###
    # Für Serial-Datentypen sind Unique-Constraints und Standardwerte implizit durch den Datentyp festgelegt, daher können sie hier ausgelassen werden.
    if 'serial' not in data_type:
        # Für Unique-Constraints ...
        if target_column_data_type_info['is_unique']:
            # ... wird dem Datentyp das Schlüsselwort UNIQUE angehängt.
            data_type = f'{data_type} UNIQUE'
        # Auch der Standardwert kann hier festgelegt werden.
        default = target_column_data_type_info['column_default']
        # Wenn nichts angegeben ist, wird dieser automatisch auf 'NULL' gesetzt.
        # Da der Standardwert ein komplexer Ausdruck sein kann, der sich ggf. auch auf Attribute oder Tabellen bezieht, die nicht übertragen 
        # werden, wird der Standardwert in dieser ersten Version nur übernommen, wenn es sich hierbei um eine Zahl oder das SQL-Kürzel für den
        # aktuellen Zeitstempel handelt.
        if default is not None and (type(default) == int or type(default) == float or 'current_timestamp' in str(default).lower()):
            data_type = f'{data_type} DEFAULT {default}'

    # Ausgabe der zusammengefügten Anweisung zum Erstellen des neuen Attributs. Das Semikolon wird hier hinzugefügt, damit diese Anweisung der Update-Anweisung unmittelbar vorangestellt werden kann.
    return f'ALTER TABLE {table_name} ADD COLUMN {attribute_name} {data_type};'

def execute_merge_and_add_constraints(target_table_meta_data:TableMetaData, source_table_meta_data:TableMetaData, target_attribute_name:str, source_attribute_name:str, query:str, params:dict|None):
    target_engine = target_table_meta_data.engine
    try:
        execute_sql_query(target_engine, text(query), params = params, raise_exceptions = True, commit = True)
    except Exception as error:
        raise error
    else:
        try:
            return add_constraints_to_new_attribute(target_table_meta_data, source_table_meta_data, target_attribute_name, source_attribute_name)
        except Exception as error:
            raise error


def add_constraints_to_new_attribute(target_table_meta_data:TableMetaData, source_table_meta_data:TableMetaData, target_attribute_name:str, source_attribute_name:str):
    source_engine = source_table_meta_data.engine
    target_engine = target_table_meta_data.engine
    source_table_name = source_table_meta_data.table_name
    target_table_name = convert_string_if_contains_capitals_or_spaces(target_table_meta_data.table_name, source_engine.dialect.name)
    escaped_target_attribute = convert_string_if_contains_capitals_or_spaces(target_attribute_name, source_engine.dialect.name)
    data_type_info = source_table_meta_data.data_type_info[source_attribute_name]
    message = ''
    ### Hinzufügen der NOT-NULL-Constraint, falls das ursprüngliche Attribut eine solche aufweist ###
    if not data_type_info['is_nullable']:
        # Überprüfung, ob das Attribut NULL-Werte enthält (MariaDB würde dann beim Hinzufügen der Constraint Werte erzwingen, die nicht NULL sind, und
        # nur eine Warnung ausgeben)
        null_count = execute_sql_query(target_engine, text(f'SELECT COUNT(*) FROM {target_table_name} WHERE {escaped_target_attribute} IS NULL')).fetchone()[0]
        if null_count > 0:
            message = f'Dem Attribut {target_attribute_name} kann keine NOT-NULL-Constraint hinzugefügt werden, da darin NULL-Werte enthalten sind.'
        else:
            query = None
            # Für MariaDB müssen beim Hinzufügen einer NOT-NULL-Constraint alle Attributinformationen wie der Standardwert angegeben werden,
            # damit diese erhalten bleiben.
            if target_engine.dialect.name == 'mariadb':
                data_type = get_full_column_definition_for_mariadb(target_table_meta_data, target_attribute_name)
                if data_type is not None:
                    query = f'ALTER TABLE {target_table_name} MODIFY {data_type} NOT NULL'
            elif target_engine.dialect.name == 'postgresql':
                query = f'ALTER TABLE {target_table_name} ALTER COLUMN {escaped_target_attribute} SET NOT NULL'
            else:
                raise DialectError(f'Der SQL-Dialekt {target_engine.dialect.name} wird nicht unterstützt.')
            try:
                execute_sql_query(source_engine, text(query), raise_exceptions = True, commit = True)
            except Exception:
                message = 'Aufgrund eines Fehlers konnte die NOT-NULL-Constraint nicht hinzugefügt werden.'
            else:
                message = 'Die NOT-NULL-Constraint konnte erfolgreich hinzugefügt werden.'

    ### CHECK-Constraints ###
    if source_engine.dialect.name == 'mariadb':
        # Abfrage von https://dataedo.com/kb/query/mariadb/list-table-check-constraints, abgewandelt
        constraint_query = f"SELECT CHECK_CLAUSE, CONSTRAINT_NAME FROM information_schema.check_constraints WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = '{source_table_name}' AND CHECK_CLAUSE LIKE '%\"{source_attribute_name}\"%'"
    elif source_engine.dialect.name == 'postgresql':
        escaped_source_attribute = convert_string_if_contains_capitals_or_spaces(source_attribute_name, source_engine.dialect.name)
        # Abfrage von https://dba.stackexchange.com/questions/214863/how-to-list-all-constraints-of-a-table-in-postgresql User David V McKay, abgewandelt
        constraint_query = f"SELECT pg_catalog.pg_get_constraintdef(r.oid, true) as condef, conname FROM pg_catalog.pg_constraint r WHERE r.conrelid in ('{source_table_name}'::regclass) AND pg_catalog.pg_get_constraintdef(r.oid, true) LIKE 'CHECK (%{escaped_source_attribute}'"
    else:
        raise DialectError(f'Der SQL-Dialekt {source_engine.dialect.name} wird nicht unterstützt.')
    constraint_result = convert_result_to_list_of_lists(execute_sql_query(source_engine, text(constraint_query)))
    if len(constraint_result) > 0:
        success_counter = 0
        for row in constraint_result:
            constraint_string = str(row[0]).replace(source_attribute_name, target_attribute_name)
            constraint_name = str(row[1]).replace(source_attribute_name, target_attribute_name)
            if not constraint_string.lower().startswith('check'):
                constraint_string = f'CHECK ({constraint_string})'
            add_constraint_query = text(f'ALTER TABLE {target_table_name} ADD CONSTRAINT {constraint_name} {constraint_string}')
            try:
                execute_sql_query(target_engine, add_constraint_query, raise_exceptions = True, commit = True)
            except Exception as error:
                message = f'{message} Die Bedingung {constraint_string} konnte aufgrund eines Fehlers nicht hinzugefügt werden: {str(error)}.'
            else:
                success_counter += 1
        if success_counter == len(constraint_result):
            message = f'{message} Alle {success_counter} CHECK-Constraints konnten erfolgreich hinzugefügt werden.'
        elif success_counter != 0:
            message = f'{message} {success_counter} von {len(constraint_result)} CHECK-Constraints konnten erfolgreich hinzugefügt werden.'
    else:
        if data_type_info['is_nullable']:
            message = f'Für das Attribut {source_attribute_name} bestehen in der Quelltabelle keine NOT-NULL- oder CHECK-Constraints.'
        else:
            message = f'Für das Attribut {source_attribute_name} bestehen in der Quelltabelle keine CHECK-Constraints.'
    return message


def get_full_column_definition_for_mariadb(table_meta_data:TableMetaData, attribute_name:str, ):
    if attribute_name not in table_meta_data.columns:
        raise ArgumentError(None, 'Das zu betrachtende Attribut muss in der angegebenen Tabelle enthalten sein.')
    engine = table_meta_data.engine
    table_name = table_meta_data.table_name
    if engine.dialect.name != 'mariadb':
        raise DialectError(f'Der SQL-Dialekt {engine.dialect.name} wird in dieser Funktion nicht unterstützt.')
    result = execute_sql_query(engine, text(f'SHOW CREATE TABLE {table_name}'))
    if result is not None:
        create_statements = str(result.fetchone()[1]).split(',')
        for statement in create_statements:
            # Die erste mit dem Namen des gesuchten Attributs beginnende Zeile in der 'CREATE-TABLE'-Anweisung enthält dessen Definition
            if statement.startswith(attribute_name) or statement.startswith(f'"{attribute_name}"'):
                return statement
    return None


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
    elif len(attributes_to_select_1) + len(attributes_to_select_2) == 0:
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

def check_basic_data_type_compatibility(table_meta_data_1:TableMetaData, table_meta_data_2:TableMetaData):
    compatibility_by_code = {}
    for column_name_1 in table_meta_data_1.columns:
        for column_name_2 in table_meta_data_2.columns:
            full_dtype_info_1 = table_meta_data_1.data_type_info[column_name_1]
            full_dtype_info_2 = table_meta_data_2.data_type_info[column_name_2]
            dgroup_1 = table_meta_data_1.get_data_type_group(column_name_1)
            dgroup_2 = table_meta_data_2.get_data_type_group(column_name_2)
            dtype_1 = table_meta_data_1.get_data_type(column_name_1).lower()
            dtype_2 = table_meta_data_2.get_data_type(column_name_2).lower()
            # Code für Kompatibilität. 0 bei fehlender Kompatibilität; 1 bei voller Kompatibilität; 2 bei ggf. uneindeutigen Einträgen des Attributs; 3, wenn ggf. Typkonversionen nötig sind; 4, wenn definitiv Typkonversionen notwendig sind. Durch Kombination können sich zudem die Werte 5 für ggf. nicht eindeutige Werte mit ggf. nötigen Typkonversionen und 6 für ggf. nicht eindeutige Werte mit nötigen Typkonversionen ergeben.
            comp_code = 0
            print(full_dtype_info_1, full_dtype_info_2)
            if (full_dtype_info_1 == full_dtype_info_2) or (dgroup_1 == dgroup_2 and full_dtype_info_1['is_unique'] and full_dtype_info_2['is_unique']):
                comp_code = 1
            else:
                if not full_dtype_info_1['is_unique'] or not full_dtype_info_2['is_unique']:
                    comp_code = 2
                if dgroup_1 != dgroup_2:
                    if dgroup_1 in ('integer', 'decimal') and dgroup_2 in ('integer', 'decimal'):
                        comp_code += 3
                    elif dgroup_1 in ('integer', 'boolean', 'decimal', 'text', 'date') and dgroup_2 in ('integer', 'boolean', 'decimal', 'text', 'date'):
                        comp_code += 4
            if comp_code not in compatibility_by_code.keys():
                compatibility_by_code[comp_code] = [(column_name_1, column_name_2)]
            else:
                compatibility_by_code[comp_code].append((column_name_1, column_name_2))

    return compatibility_by_code