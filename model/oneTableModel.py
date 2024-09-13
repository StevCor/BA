
from argparse import ArgumentError
from sqlalchemy import bindparam, text
from ControllerClasses import TableMetaData
from model.SQLDatabaseError import DialectError, QueryError, UpdateError
from model.databaseModel import build_sql_condition, check_database_encoding, convert_result_to_list_of_lists, convert_string_if_contains_capitals_or_spaces, execute_sql_query, get_full_table_ordered_by_primary_key


##### Funktionen für die Suche in einer Tabelle #####

def search_string(table_meta_data:TableMetaData, string_to_search:str, columns_to_search:list[str]):
    """Erstellen und Ausführen der Abfrage für die Suche nach einem String in einer Tabelle
    
    table_meta_data: TableMetaData-Objekt der zu durchsuchenden Tabelle
     
    string_to_search: zu suchende Zeichenkette
    
    columns_to_search: Liste mit den Attributnamen (als Strings), die durchsucht werden sollen

    Ausgabe des Abfrageergebnisses als Liste von Listen; Ausgabe eines DialectErrors bei nicht unterstützten SQL-Dialekten."""

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
    # Ausgabe des Ergebnisses der Suche
    return result


##### Funktionen für das Ersetzen von (Teil-)Strings #####

def get_replacement_information(table_meta_data:TableMetaData, affected_attributes_and_positions:list[tuple[str, int:0|1]], old_value:str, replacement:str):
    """Ermittlung der Zeilennummern, der Positionen der betroffenen Attribute sowie ihrer alten und ihrer neuen Werte für die Ersetzung von (Teil-)Strings.
    
    table_meta_data: TableMetaData-Objekt der zu durchsuchenden Tabelle
    
    affected_attributes_and_positions: Liste zweistelliger Tupel; eines für jedes Attribut der betroffenen Tabelle. An der ersten Position des
    Tupels steht der Name des Attributs, an zweiter Stelle der Wert 1, falls das Attribut von der Ersetzung betroffen sein kann, anderenfalls der
    Wert 0.
    
    old_value: zu ersetzender Wert als String
    
    replacement: der neu einzutragende String

    Ausgabe zweier Dictionarys: eines zur Identifizierung der betroffenen Tupel der Tabelle, ihrer alten und neuen Werte sowie eines zur eindeutigen
    Identifizierung der Vorkommen des gesuchten Strings für den Fall, dass mehrere Attribute durchsucht wurden; Ausgabe eines ArgumentErrors, wenn
    kein TableMetaData-Objekt übergeben wurde, min. ein Positionswert weder 0 noch 1 ist oder keine Attribute von der Änderung betroffen sein können."""
    
    cols_and_dtypes = table_meta_data.data_type_info
    # Die Länge der Positionsliste muss mit der Anzahl aller Attribute in der betroffenen Tabelle übereinstimmen.
    if len(affected_attributes_and_positions) != len(cols_and_dtypes.keys()):
        raise ArgumentError(None, 'Für alle Attribute der Tabelle muss angegeben sein, ob sie von der Änderung betroffen sein können oder nicht.')
    # Nicht betroffene Attribute müssen durch den Wert 0 in der Postionsliste gekennzeichnet sein; betroffene durch den Wert 1.
    if any([x[1] != 0 and x[1] != 1 for x in affected_attributes_and_positions]):
        raise ArgumentError(None, 'Kann ein Attribut von der Änderung betroffen sein, muss dies durch den Wert 1 in der Liste gekennzeichnet sein. Anderenfalls sollte dort der Wert 0 stehen.')
    
    ### Auflistung der betroffenen Attribute und Herausfiltern der Positionsliste aus affected_attributes_and_positions ###
    affected_attributes = []
    positions = []
    for item in affected_attributes_and_positions:
        # Wenn der Positionswert des aktuellen Attributs 1 ist, ...
        if item[1]:
            # ... wird es in die Liste der betroffenen Attribute aufgenommen
            affected_attributes.append(item[0])
        # Außerdem wird der Positionswert in eine neue Positionsliste übertragen.
        positions.append(item[1])
    
    ### Ermittlung der Tupel und Attribute, in denen der zu ersetzende Wert vorkommt ###
    # Anlegen eines Dictionarys, in dem die Vorkommen des zu ersetzenden Wertes gespeichert werden
    occurrence_dict = {}
    # Beziehen der Primärschlüsselattribute
    primary_keys = table_meta_data.primary_keys
    # Wenn kein Attribut der Tabelle von der Änderung betroffen sein kann, wird hierzu eine Fehlermeldung ausgegeben.
    if len(affected_attributes) < 1:
        raise ArgumentError('Es muss mindestens ein Attribut angegeben sein, dessen Werte bearbeitet werden sollen.')
    
    ### Ermittlung der betroffenen Tupel, wenn mehr als ein involviert sein kann ###
    elif len(affected_attributes) > 1:
        # unveränderte Tabelle zur Ermittlung der alten Werte für die Anzeige
        unaltered_table = get_full_table_ordered_by_primary_key(table_meta_data, convert = False)
        # vollständige Attributliste der Tabelle
        all_attributes = list(unaltered_table.keys())

        ### Ermittlung der Positionen der Primärschlüsselattribute im Abfrageergebnis für die unverönderte Tabelle ###
        primary_key_indexes = []
        for index, key in enumerate(all_attributes):
            if key in primary_keys:
                primary_key_indexes.append(index)

        # Umwandlung der unveränderten Tabelle in eine Liste von Listen, damit sie ggf. mehrfach durchiteriert werden kann
        unaltered_table = convert_result_to_list_of_lists(unaltered_table)
        # Simulation der Ersetzung aller Vorkommen des Strings, um die neuen Werte für die Anzeige zu ermitteln
        table_with_full_replacement = replace_all_string_occurrences(table_meta_data, affected_attributes, old_value, replacement, commit = False)
        # Beziehen eines Dictionarys mit den Zeilennummern aller betroffenen Tupel als Schlüssel und einer Liste als Wert, die an der Position
        # jedes Attributs der Tabelle den Wert 1 enthält, wenn es den gesuchten Wert enthält, sonst 0.
        row_nos_old_and_new_values = get_indexes_of_affected_attributes_for_replacing(table_meta_data, old_value, affected_attributes)

        ### Erstellen des Dictionarys zur Zuordnung der alten und der neuen Werte sowie der Primärschlüsselwerte zu den Vorkommen des gesuchten Strings ###
        # Zähler für die Vorkommen des gesuchten Wertes
        occurrence_counter = 0
        primary_key_value = []
        # Für alle betroffenen Tupel wird nun ermittelt, welche ihrer Attribute den gesuchten Wert enthalten.
        for row_no in row_nos_old_and_new_values.keys():
            # Da die unveränderte Tabelle und die Tabelle zur Ermittlung der Zeilennummern beide nach den Primärschlüsselattributen geordnet waren,
            # stehen die alten Werte jedes betroffenen Tupels in der unveränderten Tabelle an derselben Zeilenposition (-1, weil die SQL-Funktion
            # ROW_NUMBER bei 1 anfängt zu zählen, List-Indizes jedoch bei 0 beginnen).
            old_values = list(unaltered_table[row_no-1])
            # Die Positionsliste (aus Nullen und Einsen) wird aus dem Index-Dictionary übernommen.
            positions = row_nos_old_and_new_values[row_no]
            for index in positions:
                # Ist der Wert in dieser Liste nicht 0, handelt es sich um ein Vorkommen des gesuchten Wertes.
                if index != 0:
                    # Daher wird der Vorkommenszähler erhöht.
                    occurrence_counter += 1
                    # Außerdem werden die Primärschlüsselwerte des Tupels zu dessen eindeutiger Identifizierung für die Ausführung der Ersetzung ermittelt.
                    for pk_index in primary_key_indexes:
                        primary_key_value.append(old_values[pk_index])
                    # Anschließend wird dem Vorkommens-Dictionary ein neuer Eintrag hinzugefügt: mit der laufenden Nummer des Vorkommens als
                    # Schlüssel und einem weiteren Dictionary mit der Zeilennumer, den Primärschlüsselwerten und dem Namen des betroffenen Attributs als Wert.
                    occurrence_dict[occurrence_counter] = {'row_no': row_no, 'primary_key': primary_key_value, 'affected_attribute': all_attributes[index]}
            # Die neuen Werte für das jeweilige Tupel werden der Tabelle entnommen, in der alle Ersetzungen simuliert wurden.
            new_values = list(table_with_full_replacement[row_no-1])
            ### In dieser Liste werden alle nicht von der Änderung betroffenen Werte zur leichteren Identifizierung mit 'None' ersetzt. ###
            for index in range(len(old_values)):
                if not positions[index]:
                    new_values[index] = None
            # Anschließend erhält das Dictionary zur Identifizierung der betroffenen Tupel einen neuen Eintrag: Über die Zeilennummer identifiziert
            # wird ein weiteres Dictionary eingetragen, das die alten Werte des Tupels, die Positionsliste, die Liste der neuen Werte und die 
            # Primärschlüsselwerte zur eindeutigen Identifizierung enthält.
            row_nos_old_and_new_values[row_no] = {'old': old_values, 'positions': positions, 'new': new_values, 'primary_key': primary_key_value}
    
    ### Ermittlung der betroffenen Tupel, wenn nur ein Attribut involviert sein kann ### 
    else:
        ### Wie zuvor werden die Tabelle mit allen Ersetzungen und die Nummern der betroffenen Tupel bezogen ###
        attribute_with_full_replacement = replace_all_string_occurrences(table_meta_data, affected_attributes, old_value, replacement, commit = False)
        affected_row_nos_and_unaltered_entries = get_row_number_of_affected_entries(table_meta_data, affected_attributes, [old_value], 'replace', convert = False)
        row_nos_old_and_new_values = {}
        # Hier ist jedoch nur ein Attribut betroffen, sodass keine Liste erforderlich ist.
        affected_attribute_no = None

        ##### Erstellung des Vorkommens-Dictionarys #####
        occurrence_counter = 0
        ### Ermittlung der Position des für die Ersetzung ausgewählten Attributs im Abfrageergebnis der vollen Ersetzung ###
        for index, key in enumerate(affected_row_nos_and_unaltered_entries.keys()):
            if key == affected_attributes[0]:
                # Abzug von 1, weil das Abfrageergebnis zusätzlich zu den Attributen der Tabelle die Zeilennummer an erster Stelle enthält
                affected_attribute_no = index - 1
                # Abbruch der Schleife, da nur ein Attribut durchsucht wird
                break
        for row in affected_row_nos_and_unaltered_entries:
            ### Ermittlung der Primärschlüsselwerte des aktuellen Tupels ###
            primary_key_value = []
            for index, key in enumerate(affected_row_nos_and_unaltered_entries.keys()):
                if key in primary_keys:
                    primary_key_value.append(row[index])
            # Die Tupelnummer steht im ersten Eintrag der aktuellen Zeile des Abfrageergebnisses.
            row_no = row[0]
            # Somit enthalten alle anderen Einträge der Zeile die alten Werte, die als Liste übernommen werden.
            old_values = list(row[1:])
            ### Ermittlung der Liste der neuen Werte ###
            # Erstellen einer Liste mit einem Eintrag 'None' für jedes Attribut der Tabelle
            new_values = [None] * len(old_values)
            # An der Position des betroffenen Attributs wird in diese der entsprechende neue Wert eingetragen.
            new_values[affected_attribute_no] = attribute_with_full_replacement[row_no-1][0]
            ### Zusammenfügen der Ausgabe-Dictionarys ###
            row_nos_old_and_new_values[row_no] = {'old': old_values, 'positions': positions, 'new': new_values, 'primary_key': primary_key_value}
            occurrence_counter += 1
            occurrence_dict[occurrence_counter] = {'row_no': row_no, 'primary_key': primary_key_value, 'affected_attribute': affected_attributes[0]}
    # Ausgabe der vollständigen Dictionarys
    return row_nos_old_and_new_values, occurrence_dict

def replace_all_string_occurrences(table_meta_data:TableMetaData, column_names:list, string_to_replace:str, replacement_string:str, commit:bool = False):
    """Ersetzt alle Vorkommen des gesuchten Wertes, für Strings auch als Teilersetzungen und gibt die aktualisierte Tabelle aus.
    
    table_meta_data: TableMetaData-Objekt der betroffenen Tabelle
    
    column_names: Liste der zu betrachtenden Attribute
    
    string_to_replace: zu ersetzender Wert als String
    
    replacement_string: neu einzutragender Wert als String
    
    commit: Flag für das Schreiben der Änderung in die Datenbank. Das Ergebnis der Anweisung wird nur gespeichert, wenn dieser Parameter True ist.

    Ausgabe der gesamten aktualisierten Tabelle (bzw. des aktualisierten Attributs, wenn nur eines ausgewählt wurde) als Liste von Listen; Ausgabe
    eines UpdateErrors, wenn bei der Abfrageausführung Fehler auftreten."""

    ### Vorbereiten der Variablen für die Datenbankabfrage, ggf. mit Escaping ###
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, db_dialect)
    string_to_replace = escape_string(db_dialect, string_to_replace)
    replacement_string = escape_string(db_dialect, replacement_string)
    primary_keys = ', '.join([convert_string_if_contains_capitals_or_spaces(key, db_dialect) for key in table_meta_data.primary_keys])

    ### Aufbau der Abfrage und des Parameter-Dictionarys ###
    update_params = {}

    query = f'UPDATE {table_name} SET'
    ## Aufbau des Abfragenabschnitts, in dem der einzutragende Wert steht ##
    for index, column_name in enumerate(column_names):
        column_name = convert_string_if_contains_capitals_or_spaces(column_name, db_dialect)
        data_type_group = table_meta_data.get_data_type_group(column_name)
        ## Im Fall nicht textbasierter Datentypen wird der neue Wert als vollständiger Ersatz für den alten eingetragen ##
        if data_type_group != 'text':
            # Einfügen des Platzhalters für den neuen Wert in die Abfrage
            query = f"{query} {column_name} = :new_value_{str(index)}"
            ## Eintragen des ggf. konvertierten neuen Wertes in das Parameter-Dictionary ##
            if data_type_group == 'integer':
                update_params[f'new_value_{str(index)}'] = int(replacement_string)
            elif data_type_group == 'decimal':
                update_params[f'new_value_{str(index)}'] = float(replacement_string)
            elif data_type_group == 'boolean':
                update_params[f'new_value_{str(index)}'] = bool(replacement_string)
            else:
                update_params[f'new_value_{str(index)}'] = replacement_string
        ## Im Fall textbasierter Datentypen wird eine Ersetzung des gesuchten (Teil-)Strings mit regexp_replace vorgenommen. ##
        else:
             ## Ergänzung von Flags, damit das Matching mittels regulärer Ausdrücke in MariaDB und PostgreSQL gleich erfolgt ##
            postgres_flag = ''
            old_value = string_to_replace
            if db_dialect == 'postgresql':
                postgres_flag = ", 'g'"
            # In MariaDB muss dem alten Wert das Flag '(?-i)' vorangestellt werden, damit das Matching unabhängig von der Tabellenkollation unter
            # Berücksichtigung von Groß- und Kleinschreibung erfolgt.
            elif db_dialect == 'mariadb':
                old_value = f'(?-i){string_to_replace}'

            # Erstellung der Abfrage    
            query = f"{query} {column_name} = regexp_replace({column_name}, :old_value, :new_value{postgres_flag})"
            # Wenn noch keine Eintragung dieser Art im Parameter-Dictionary vorgenommen wurde, ...
            if 'old_value' not in update_params.keys():
                # ... werden der zu ersetzende ...
                update_params['old_value'] = old_value
                # ... und der neue String in das Dictionary eingetragen.
                update_params['new_value'] = replacement_string
        # Wenn es sich nicht um den letzten aufzulistenden Wert handelt, wird dem Ausdruck ein Komma angehängt.
        if index != len(column_names)-1:
            query = f'{query},' 
    # Da alle Vorkommen ersetzt werden, enthält die Anweisung keine Bedingung.
    query = text(query)
    ### Ausführung der Abfrage ###
    # Binden der Parameter 
    for key in update_params.keys():
        query.bindparams(bindparam(key))
    try:
        ## Ausführung der Änderung ##
        connection = engine.connect()
        connection.execute(query, update_params)
        ## Anschließend wird das Ergebnis der Änderung über dieselbe Verbindung abgefragt. ##
        # Wurde nur ein zu betrachtendes Attribut angegeben, wird nur dieses abgefragt.
        if len(column_names) == 1:
            column_name = convert_string_if_contains_capitals_or_spaces(column_names[0], db_dialect)
            result = connection.execute(text(f'SELECT {column_name} FROM {table_name} ORDER BY {primary_keys}'))
        # Anderenfalls werden alle Attribute der Tabelle abgefragt.
        else:
            result = connection.execute(text(f'SELECT * FROM {table_name} ORDER BY {primary_keys}'))
    # Treten Fehler auf, wird die gesamte Transaktion rückgängig gemacht ...
    except Exception as error:
        connection.rollback()   
        # ... und ein UpdateError ausgegeben.     
        raise UpdateError(str(error))
    else:
        # Bei Erfolg wird die Aktion in die Datenbank geschrieben, wenn gewünscht.
        if commit:
            connection.commit()
        # Anderenfalls wird die Transaktion ebenfalls rückgängig gemacht. 
        else:
            connection.rollback()
    ## Schließen der Verbindung ##
    finally:
        try:
            connection.close()
        # Fehler treten hierbei üblicherweise auf, wenn keine Verbindung aufgebaut werden konnte. Da eine nicht geöffnete Verbindung nicht 
        # geschlossen werden muss, kann dies ignoriert werden.
        except Exception as error:
            pass
    # Ausgabe der aktualisierten Tabelle (bzw. nur des aktualisierten Attributs) als Liste von Listen
    return convert_result_to_list_of_lists(result)

def get_indexes_of_affected_attributes_for_replacing(table_meta_data:TableMetaData, old_value:str, affected_attributes:list = None):
    """Ermittlung der Tupel und der Attribute, die den zu ersetzenden Wert enthalten.
    
    table_meta_data: TableMetaData-Objekt der betroffenen Tabelle
    
    old_value: zu ersetzender Wert als String
    
    affected_attributes: Liste der Attribute, die von der Ersetzung betroffen sein können, optional

    Ausgabe eines Dictionarys, das die Nummer der betroffenen Tupel als Schlüssel und eine Liste von Nullen und Einsen als Wert enthält. In Letzterer
    bedeutet jede Eins, dass das Attribut an dieser Position den zu ersetzenden Wert enthält."""

    ### für die Abfrage benötigte Objekte und Tabellenmetadaten ###
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, db_dialect)
    cols_and_dtypes = table_meta_data.data_type_info
    primary_keys = table_meta_data.primary_keys
    string_to_replace = escape_string(db_dialect, old_value)
    # Anlegen eines Parameter-Dictionarys, damit der gesuchte String mittels bindparams gegen SQL-Injektion abgesichert in die Abfrage eingefügt 
    # werden kann
    params_dict = {'old_value': string_to_replace}
    # Auflistung der Primärschlüsselattribute als String zur Einbindung in die Abfrage
    keys = ', '.join(convert_string_if_contains_capitals_or_spaces(key, db_dialect) for key in primary_keys)

    ### Aufbau der Abfrage ###
    query = 'SELECT'
    # Sie beruht auf einer CASE-Anweisung, die für jede Zelle mit dem zu ersetzenden Wert eine Eins ausgibt und sonst eine Null.
    case_selected_attribute = 'THEN 1 ELSE 0 END'
    case_nonselected_attribute = '0'
    # Setzen des dialektspezifischen Operators und Konversionsdatentyps für die Suche mit regulären Ausdrücken
    operator, cast_data_type = set_matching_operator_and_cast_data_type(db_dialect)
    # Einbindung des Parameternamens in den regulären Ausdruck
    concatenated_string = get_concatenated_string_for_matching(db_dialect, 'old_value')
    condition = f"{operator} {concatenated_string}"
    for index, key in enumerate(cols_and_dtypes.keys()):
        # Wenn keine betroffenen Attribute angegeben sind, werden alle als potenziell betroffen betrachtet und daher durchsucht. Anderenfalls
        # wird die Bedingung, die den Wert 1 ergeben kann, nur für die angegebenen zu durchsuchenden Attribute in die Abfrage eingefügt.
        if affected_attributes is None or (affected_attributes is not None and key in affected_attributes):
            # Versehen des aktuell betrachteten Attributnamens mit Trennzeichen, falls nötig
            escaped_attribute = convert_string_if_contains_capitals_or_spaces(key, db_dialect)
            # Alle nicht textbasierten Attribute werden für diese Abfrage in Text konvertiert.
            if table_meta_data.get_data_type_group(key) != 'text':
                query = f'{query} CASE WHEN CAST({escaped_attribute} AS {cast_data_type}) {condition} {case_selected_attribute}'
            # Für textbasierte Attribute entfällt die Konversion.
            else:
                query = f'{query} CASE WHEN {escaped_attribute} {condition} {case_selected_attribute}'
        # Für Attribute, die von der Suche ausgeschlossen wurden, wird stets der Wert 0 ausgegeben.
        else: 
            query = f'{query} {case_nonselected_attribute}'
        # Wenn es sich bei dem aktuell betrachteten Attribut nicht um das letzte der Tabelle handelt, wird der Abfrage zur Abtrennung des folgenden
        # Attributs ein Komma angefügt.
        if index < len(cols_and_dtypes.keys())-1:
            query = f'{query},'

    ### Zusammensetzen und Ausführen der Abfrage, die nach Primärschlüsseln geordnet erfolgt ###
    query = f'{query} FROM {table_name} ORDER BY {keys}'
    # In MariaDB muss die Abfrage mit der binären Version der Datenbankkollation ausgeführt werden, damit das Matching unter Berücksichtigung
    # von Groß- und Kleinschreibung erfolgt.
    if db_dialect == 'mariadb':
        query = f'{query} COLLATE {check_database_encoding(engine)}_bin'
    # Ausführen der Abfrage
    result = execute_sql_query(engine, text(query), params_dict)

    ### Aufbau des Rückgabe-Dictionarys ###
    row_ids = dict()
    for index, row in enumerate(result):
        print(row)
        # Für alle Ergebniszeilen der Abfrage, die nicht nur Nullen enthalten, ...
        if sum(row) != 0:
            # ... wird dem Dictionary ein Eintrag mit der Zeilenummer als Schlüssel (index + 1, da die SQL-Funktion ROW_NUMBER() ab 1 zählt) und
            # der Ergebniszeile als Wert (konvertiert in eine Liste, damit die Einträge verändert werden können) angehängt.
            row_ids[index+1] = list(row)
    # Ausgabe des Dictionarys
    return row_ids

#notest
def replace_some_string_occurrences(table_meta_data:TableMetaData, occurrences_dict:dict, string_to_replace:str, replacement_string:str, commit:bool = False):
    """Ersetzt die ausgewählten Vorkommen des gesuchten Wertes und gibt eine Meldung mit einer Statistik zu Erfolgen und Fehlschlägen aus.
    
    table_meta_data: TableMetaData-Objekt der betroffenen Tabelle
    
    occurrences_dict: Dictionary mit den zur Ersetzung benötigten Informationen (Name des betroffenen Attributs, Primärschlüsselwert des Tupels
    mit dem Vorkommen); wird in der Form erwartet, die get_replacement_information als zweiten Wert ausgibt, ergänzt um einen Eintrag mit dem
    Schlüssel 0, der ein Dictionary mit dem Schlüssel 'primary_keys' und einer Liste der Primärschlüsselwerte des betroffenen Tupels als Wert enthalt.
    
    string_to_replace: zu ersetzender Wert als String
    
    replacement_string: neu einzutragender Wert als String
    
    commit: Flag für das Schreiben der Änderung in die Datenbank. Das Ergebnis der Anweisung wird nur gespeichert, wenn dieser Parameter True ist.
    
    Ausgabe einer Meldung über den (Miss-)Erfolg der Operation, die in der App angezeigt werden kann. Bei Fehlern sind hierin die 
    Primärschlüsselwerte der von den Fehlern betroffenen Tupeln angegeben."""

    ### Vorbereiten der Variablen für die Datenbankabfrage, ggf. mit Escaping ###
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, db_dialect)
    string_to_replace = escape_string(db_dialect, string_to_replace)
    replacement_string = escape_string(db_dialect, replacement_string)

    # Beziehen der Primärschlüssel aus dem Vorkommens-Dictionary
    primary_key_attributes = occurrences_dict[0]['primary_keys']
    # Entfernen der Primärschlüssel aus dem Dictionary
    occurrences_dict.pop(0)

    ### Ausführung der Änderung in einer eigenen Abfrage für jedes Vorkommen ###
    # Zähler für die erfolgreichen Änderungen
    success_counter = 0
    # Liste der Primärschlüsselwerte der Vorkommen, die nicht geändert werden konnten
    failed_updates = []
    for row in occurrences_dict.values():
        ## Aufbau der Abfrage ##
        query = f'UPDATE {table_name} SET'
        update_params = {}
        # Primärschlüsselwerte des Tupels mit dem aktuell betrachteten Vorkommen des gesuchten Wertes
        primary_key_value = row['primary_key']
        # Name des Attributs, in dem das aktuelle Vorkommen enthalten ist und ... 
        affected_attribute = convert_string_if_contains_capitals_or_spaces(row['affected_attribute'], db_dialect)
        # ... dessen Datentypgruppe
        data_type_group = table_meta_data.get_data_type_group(affected_attribute)
        # Bei nicht textbasierten Datentypen wird der gesuchte Wert vollständig durch den neuen Wert ersetzt
        if data_type_group != 'text':
            query = f"{query} {affected_attribute} = :new_value"
            ## Der in das Parameter-Dictionary eingefügte Wert wird für Zahlen und Boolean-Werte in den dem Attributdatentyp entsprechenden 
            # Python-Datentyp konvertiert ...
            if data_type_group == 'integer':
                update_params[f'new_value'] = int(replacement_string)
            elif data_type_group == 'decimal':
                update_params[f'new_value'] = float(replacement_string)
            elif data_type_group == 'boolean':
                update_params[f'new_value'] = bool(replacement_string)
            # ... und für alle anderen nicht textbasierten Datentypen unverändert übernommen.
            else:
                update_params[f'new_value'] = replacement_string
        else:
            ## Ergänzung von Flags, damit das Matching mittels regulärer Ausdrücke in MariaDB und PostgreSQL gleich erfolgt ##
            postgres_flag = ''
            old_value = string_to_replace
            # In PostgreSQL muss das Flag 'g' als vierter Parameter in regexp_replace eingesetzt werden, damit wie in MariaDB alle Vorkommen des 
            # gesuchten Wertes ersetzt werden, nicht nur das erste.
            if db_dialect == 'postgresql':
                postgres_flag = ", 'g'"
            # In MariaDB muss dem alten Wert das Flag '(?-i)' vorangestellt werden, damit das Matching unabhängig von der Tabellenkollation unter
            # Berücksichtigung von Groß- und Kleinschreibung erfolgt.
            elif db_dialect == 'mariadb':
                old_value = f'(?-i){string_to_replace}'
            
            # Aufbau der Abfrage
            query = f"{query} {affected_attribute} = regexp_replace({affected_attribute}, :old_value, :replacement_string{postgres_flag})"
            ## Eintragen der alten und der neuen Werte in das Parameter-Dictionary ##
            update_params['old_value'] = old_value
            update_params['replacement_string'] = replacement_string
        
        ### Aufbau der Bedingung zur eindeutigen Identifizierung der zu ersetzenden Vorkommen anhand ihrer Primärschlüsselwerte ###
        for index, key in enumerate(primary_key_attributes):
            # Einfügen der Primärschlüsselwerte in das Parameter-Dictionary
            update_params[key] = primary_key_value[index]
        # Erstellen der Bedingung
        condition = build_sql_condition(tuple(primary_key_attributes), db_dialect, 'AND')

        ### Zusammenfügen und Ausführen der UPDATE-Anweisung ###
        query = text(f'{query} {condition}')
        print(query)
        try:
            execute_sql_query(engine, query, update_params, raise_exceptions = True, commit = commit)
        # Bei Fehlern werden die Primärschlüsselwerte des aktuellen Vorkommens in die Liste der fehlgeschlagenen Änderungen aufgenommen.
        except Exception:
            failed_updates.append(primary_key_value)
        # Anderenfalls konnte die Änderung erfolgreich durchgeführt werden.
        else:
            success_counter += 1
    ### Wenn der Erfolgszähler nach Abschluss der Schleife der Anzahl der ausgewählten Vorkommen entspricht, waren alle Ersetzungen erfolgreich.
    # Daher wird eine Erfolgsmeldung mit der Gesamtanzahl der Ersetzungen ausgegeben. ###
    if success_counter == len(occurrences_dict):
        if len(occurrences_dict) == 1:
            return f'Der ausgewählte Wert wurde erfolgreich aktualisiert.'
        else:
            return f'Alle {len(occurrences_dict)} ausgewählten Werte wurden erfolgreich aktualisiert.'
    ### Wenn mindestens eine Ersetzung fehlgeschlagen ist, wird eine Meldung mit der Anzahl der Fehlschläge und ihren Primärschlüsselwerten ausgegeben,
    # anhand derer eine manuelle Fehlersuche erfolgen kann. ###
    else:
        if success_counter == 1:
            verb = 'wurde'
        else:
            verb = 'wurden'
        return f'{success_counter} von {len(occurrences_dict)} betroffenen Werten {verb} erfolgreich aktualisiert. Fehler sind in den Zeilen mit folgenden Primärschlüsselwerten aufgetreten: {failed_updates}. Bitte sehen Sie sich diese nochmal an.'
    

### Funktionen für das Vereinheitlichen von Datenbankeinträgen

def get_unique_values_for_attribute(table_meta_data:TableMetaData, attribute_to_search:str):
    """Bezieht eine Liste aller einzigartigen Werte mit der Anzahl ihrer Vorkommen aus der betroffenen Tabelle.
    
    table_meta_data: TableMetaData-Objekt der betroffenen Tabelle
    
    attribute_to_search: Name des zu durchsuchenden Attributs

    Ausgabe des Abfrageergebnisses als Liste von Listen; Ausgabe eines ArgumentErrors, falls das erste Argument kein TableMetaData-Objekt ist."""
    
    # Ausgabe eines ArgumentErrors bei ungeeigneten Eingabedaten
    if type(table_meta_data) != TableMetaData:
        raise ArgumentError(None, 'Der erste übergebene Parameter muss vom Typ TableMetaData sein.')
    # Beziehen der Engine für die Abfrage
    engine = table_meta_data.engine
    ### Versehen des Tabellennamens und des zu durchsuchenden Attributs mit doppelten Anführungszeichen, falls dialektspezifisch nötig ###
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, engine.dialect.name)
    attribute_to_search = convert_string_if_contains_capitals_or_spaces(attribute_to_search, engine.dialect.name)
    # Erstellen der Abfrage; gleich für MariaDB und PostgreSQL
    query = text(f'SELECT DISTINCT {attribute_to_search}, COUNT(*) AS Eintragsanzahl FROM {table_name} GROUP BY {attribute_to_search}')
    # Ausgabe des Abfrageergebnisses als Liste von Listen
    return convert_result_to_list_of_lists(execute_sql_query(engine, query))

def update_to_unify_entries(table_meta_data:TableMetaData, attribute_to_change:str, old_values:list, new_value:str, commit:bool):
    """Simuliert die Vereinheitlichung der angegebenen Werte oder schreibt sie in die Datenbank.
    
    table_meta_data: TableMetaData-Objekt der betroffenen Tabelle
    
    attribute_to_change: Name des von der Vereinheitlichung betroffenen Attributs
    
    old_values: Liste der zu vereinheitlichenden Werte
    
    new_value: neuer Wert, durch den die Werte in old_values ersetzt werden sollen
    
    commit: Flag für das Schreiben der Änderung in die Datenbank. Bei False wird das Ergebnis der Anweisung durch Auslassen des Befehls commit() 
    nicht gespeichert.
    
    Bei Erfolg kein Rückgabewert; bei Fehlern wird ein QueryError ausgegeben."""

    # für die Abfrage benötigte Variablen
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    table_name = table_meta_data.table_name

    ##### Aufbau der Abfrage #####
    query = f'UPDATE {convert_string_if_contains_capitals_or_spaces(table_name, db_dialect)} SET {convert_string_if_contains_capitals_or_spaces(attribute_to_change, db_dialect)} = :new_value'
    cols_and_dtypes = table_meta_data.data_type_info
    # Dictionary für die Parameter der Abfragebedingung
    condition_dict = {}
    for index, key in enumerate(cols_and_dtypes.keys()):
        ### Für das zu vereinheitlichende Attribut wird der neue, als String übergebene Wert in den entsprechenden Datentyp konvertiert.
        # Fehler sollten hierbei nicht auftreten, weil die Kompatibilität auf dem Server bereits vorher getestet wurde. ###
        if key == attribute_to_change:
            data_type_group = table_meta_data.get_data_type_group(key)
            if data_type_group == 'integer':
                try:
                    new_value = int(new_value)
                except ValueError:
                    raise QueryError('Der neu eingegebene Wert kann nicht in den Datentyp integer umgewandelt werden.')
                for index, item in enumerate(old_values):
                    # Die alten Werte werden sicherheitshalber auch noch einmal in den Zieldatentyp konvertiert.
                    old_values[index] = int(item)
                # Anschließend kann die for-Schleife abgebrochen werden, da nur ein Attribut von der Vereinheitlichung betroffen ist.
                break
            elif data_type_group == 'decimal':
                try:
                    new_value = float(new_value)
                except ValueError:
                    raise QueryError('Der neu eingegebene Wert kann nicht in eine Dezimalzahl umgewandelt werden.')
                for item in enumerate(old_values):
                    old_values[index] = float(item)
                break 
            elif data_type_group == 'boolean':
                if new_value not in (0, 1, True, False):
                    raise QueryError('Der neu eingegebene Wert kann nicht sinnvoll in einen Boolean-Wert umgewandelt werden.')
                new_value = bool(new_value)
                for item in enumerate(old_values):
                    old_values[index] = bool(item)
    # Hinzufügen des neuen Wertes zum Parameter-Dictionary für die Abfrage
    condition_dict['new_value'] = new_value

    ### Aufbau der Abfragebedingung ###
    condition = 'WHERE'
    # Jedes Attribut wird ggf. mit Trennzeichen versehen und anschließend mit dem Platzhalter für den entsprechenden alten Wert gleichgesetzt.
    for index, value in enumerate(old_values):
        if index == 0:
            condition = f'{condition} {convert_string_if_contains_capitals_or_spaces(attribute_to_change, db_dialect)} = :value_{str(index)}'
        # Handelt es sich nicht um das erste Attribut, wird die neue Bedingung mit dem Operator OR mit der bereits bestehenden Bedingung verbunden.
        else:
            condition = f'{condition} OR {convert_string_if_contains_capitals_or_spaces(attribute_to_change, db_dialect)} = :value_{str(index)}'
        # Einfügen des Parameters für den Platzhalter in das Parameter-Dictionary
        condition_dict['value_' + str(index)] = value
    ### Zusammenfügen und Ausführen der Abfrage ###
    query = text(f'{query} {condition}')
    execute_sql_query(engine, query, condition_dict, True, commit)


### Hilfsfunktionen, die an mehreren Stellen verwendet werden (können) ###

def check_data_type_and_constraint_compatibility(table_meta_data:TableMetaData, column_name:str, input:str|int|float|bool, old_value:str|int|float|bool):
    """Simuliert das Einfügen des neuen Wertes in das Tupel, das den zu ersetzenden Wert als Erstes enthält, um die Kompatibilität mit
    bestehenden Attribut-Constraints zu überprüfen.
    
    table_meta_data: TableMetaData-Objekt der betroffenen Tabelle
    
    column_name: Name des betroffenen Attributs
    
    input: einzufügender Wert (vom Typ str, int, float oder bool)
    
    old_value: zu ersetzender Wert (vom Typ str, int, float oder bool)

    Bei Erfolg Ausgabe des Wertes 0; Ausgabe eines ArgumentErrors, wenn der neue der oder der zu ersetzende Wert nicht vom Typ str, int, float 
    oder bool ist, eines QueryErrors, wenn der zu ersetzende Wert nicht im betroffenen Attribut enthalten ist, sowie ggf. bei der Abfrageausführung auftretende Datenbankfehler."""

    # Überprüfung des Datentyps des neuen und des zu ersetzenden Wertes
    if type(input) not in (str, int, float, bool) or type(old_value) not in (str, int, float, bool) :
        raise ArgumentError(None, 'Datentyp kann nicht überprüft werden.')
    
    ### Vorbereitung der Variablen für die Ausführung der Abfrage ###
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, db_dialect)
    column_name = convert_string_if_contains_capitals_or_spaces(column_name, db_dialect)

    ### Einfügen des zu ersetzenden Wertes in das Parameter-Dictionary für die Abfrage ###
    update_params = {}
    update_params['old_value'] = old_value

    ### Aufbau der Abfrage des ersten Eintrags, der den zu ersetzenden Wert enthält ###
    pre_query = f'SELECT {column_name} FROM {table_name} WHERE {column_name} = :old_value OR'
    # Bezug des dialektspezifischen Operators und Konversionsdatentyps für die Suche mittels regulärer Ausdrücke
    operator, cast_data_type = set_matching_operator_and_cast_data_type(db_dialect) 
    # Einfügen des zu ersetzenden Wertes in den regulären Ausdruck
    string_to_search = get_concatenated_string_for_matching(db_dialect, 'old_value')
    # Einfügen der Bedingung für textbasierte Datentypen
    if table_meta_data.get_data_type_group(column_name) == 'text':
        pre_query = f"{pre_query} {column_name} {operator} {string_to_search} LIMIT 1"
    # Für nicht textbasierte Datentypen wird zusätzlich eine Typkonversion in den dialektspezifischen Konversionsdatentyp durchgeführt.
    else:
        pre_query = f"{pre_query} CAST({column_name} AS {cast_data_type}) {operator} {string_to_search} LIMIT 1"
    # Ausführung der Abfrage des ersten Eintrags mit dem zu ersetzenden Wert
    try:
        result = convert_result_to_list_of_lists(execute_sql_query(engine, text(pre_query), update_params, raise_exceptions = True, commit = False))
    except Exception as error:
        raise error
    else:
        # Ist das Ergebnis leer oder None, konnte der gesuchte Wert nicht im angegebenen Attribut gefunden werden, sodass der Ersetzungsvorgang
        # nicht sinnvoll ist und mit einer Fehlermeldung abgebrochen wird.
        if len(result) == 0 or result is None:
            raise QueryError(f'Der gesuchte Wert \'{old_value}\' kommt im Attribut {column_name} nicht vor.\n')
        
        # Ansonsten wird der erste Wert der ersten Zeile des Ergebnisses (d. h. der einzige Wert) für die Bedingung der Test-UPDATE-Anweisung übernommen.
        condition_value = result[0][0]

        ### Aufbau der Anweisung für die simulierte Ersetzung ###
        query = f'UPDATE {table_name} SET {column_name}'
        condition = f'WHERE {column_name} = :condition_value'

        # Für textbasierte Datentypen wird eine ggf. teilweise Ersetzung mittels der auf regulären Ausdrücken beruhenden Funktion 
        # regexp_replace ausgeführt.
        if table_meta_data.get_data_type_group(column_name) == 'text':
            ## Ergänzung von Flags, damit das Matching in MariaDB und PostgreSQL gleich erfolgt ##
            postgres_flag = ''
            value_to_replace = old_value
            # In PostgreSQL muss das Flag 'g' als vierter Parameter in regexp_replace eingesetzt werden, damit wie in MariaDB alle Vorkommen des 
            # gesuchten Wertes ersetzt werden, nicht nur das erste.
            if db_dialect == 'postgresql':
                postgres_flag = ", 'g'"
            # In MariaDB muss dem alten Wert das Flag '(?-i)' vorangestellt werden, damit das Matching unabhängig von der Tabellenkollation unter
            # Berücksichtigung von Groß- und Kleinschreibung erfolgt.
            elif db_dialect == 'mariadb':
                value_to_replace = f'(?-i){old_value}'
            # Einfügen des Parameters mit dem Flag in das Parameter-Dictionary
            update_params['old_value'] = value_to_replace
            query = f"{query} = regexp_replace({column_name}, :old_value, :new_value{postgres_flag})"
        # Für nicht textbasierte Datentypen wird der alte Eintrag des betroffenen Attributs vollständig mit dem neuen Wert ersetzt.
        else:
            query = f'{query} = :new_value'
            # In diesem Fall wird old_value aus dem Parameter-Dictionary entfernt, weil dieser Parameter nicht in der Anweisung vorkommt.
            update_params.pop('old_value')

        ### Eintragen des neuen Wertes und des abgefragten Testwertes in das Parameter-Dictionary ###  
        update_params['new_value'] = input
        update_params['condition_value'] = condition_value

        ### Fertigstellung und Ausführung der Anweisung ###
        query = text(f'{query} {condition}')
        try:
            execute_sql_query(engine, query, update_params, raise_exceptions = True, commit = False)
        except Exception as error:
            raise error
        # Ausgabe des Wertes 0, wenn bei der Ausführung keine Fehler aufgetreten sind.
        return 0

def get_row_number_of_affected_entries(table_meta_data:TableMetaData, affected_attributes:list[str], old_values:list[str], mode:str, convert:bool = True):
    """Erstellen und Ausführen einer SQL-Abfrage zur Ermittlung der Nummer der Zeilen, die von der Vereinheitlichung oder der Ersetzung betroffen sind,
    sowie ihrer aktuellen Werte.
    
    table_meta_data: TableMetaData-Objekt der betroffenen Tabelle
    
    affected_attributes: Liste der zu betrachtenden Attribute der betroffenen Tabelle
    
    old_values: Liste der bestehehenden Werte (als Strings), die ersetzt oder vereinheitlicht werden sollen
    
    mode: Ersetzungsmodus ('replace' für die Funktion 'Suchen und Ersetzen' oder 'unify' für die Vereinheitlichungsfunktion)
    
    convert: Flag für das Umwandeln des Abfrageergebnisses in eine Liste von Listen. Bei False wird das Ergebnis unverändert als CursorResult ausgegeben.
    
    Ausgabe des Abfrageergebnisses als Liste von Listen oder als CursorResult; Ausgabe eines ArgumentErrors, wenn ein ungültiger Modus angegeben ist
    oder für das Vereinheitlichen nicht genau ein Attribut oder für das Ersetzen nicht genau ein Wert angegeben ist."""

    ### Überprüfung der Form der übergebenen Argumente ###
    if not mode == 'replace' and not mode == 'unify':
        raise ArgumentError(None, 'Nur die Modi \'replace\' und \'unify\' werden unterstützt.')
    elif mode == 'unify' and len(affected_attributes) != 1:
        raise ArgumentError(None, 'Im Modus \'unify\' kann nur ein Attribut bearbeitet werden.')
    elif mode == 'replace' and len(old_values) != 1:
        raise ArgumentError(None, 'Im Modus \'replace\' kann nur ein Wert ersetzt werden.')
    
    ##### Erstellen der SQL-Anweisung #####
    ### Beziehen und Vorbereiten der Variablen (Versehen mit Trennzeichen) für die Abfrage ###
    engine = table_meta_data.engine
    db_dialect = engine.dialect.name
    table_name = convert_string_if_contains_capitals_or_spaces(table_meta_data.table_name, db_dialect)
    cols_and_dtypes = table_meta_data.data_type_info
    primary_keys = table_meta_data.primary_keys
    operator, cast_data_type = set_matching_operator_and_cast_data_type(db_dialect)
    key_for_ordering = ', '.join([convert_string_if_contains_capitals_or_spaces(key, db_dialect) for key in primary_keys])
    ### Die Zeilennummer soll als erster Eintrag jedes Tupels des Ergebnisses der Abfrage ausgegeben werden. Dahinter sollen die aktuellen Werte
    # aller Attribute der Tabelle stehen. ###
    # In PostgreSQL kann dafür hinter dem Aufruf der Funktion ROW_NUMBER() das Alias '*' verwendet werden.
    columns_to_select = '*'
    ### In MariaDB führt dies zu einem Syntaxfehler, sodass stattdessen alle Attribute der Tabelle explizit aufgelistet werden. ###
    if db_dialect == 'mariadb':
        for key in cols_and_dtypes.keys():
            if columns_to_select == '*':
                columns_to_select = convert_string_if_contains_capitals_or_spaces(key, db_dialect)
            else:
                columns_to_select = f'{columns_to_select}, {convert_string_if_contains_capitals_or_spaces(key, db_dialect)}'
    
    # Die Abfrage beginnt für beide Modi und beide Dialekte gleich.
    query = f"SELECT * FROM (SELECT ROW_NUMBER() OVER (ORDER BY {key_for_ordering}) AS Nr, {columns_to_select} FROM {table_name}) sub"

    ### Aufbau der Abfragebedingung ###
    condition = 'WHERE'
    # Parameter-Dictionary für die Abfrage
    condition_params = {}

    ## Abfragebedingung für die Ersetzung ## 
    if mode == 'replace':
        ## In diesem Fall wird nur ein Wert verändert, sodass die Liste old_values nur einen Eintrag enthält, der in das Parameter-Dictionary
        # eingetragen wird. ##
        old_value = old_values[0]
        condition_params['old_value'] = old_value
        # Die Ersetzung beruht auf einer Suche mithilfe regulärer Ausdrücke, sodass der zu suchende Wert dialektspezifisch als Parameterplatzhalter
        # in die Abfrage eingebunden werden muss. 
        concat_string = get_concatenated_string_for_matching(db_dialect, 'old_value')
        ## Aufbau der Bedingung für die zu berücksichtigenden Attribute ##
        for index, attribute in enumerate(affected_attributes):
            # Versehen des Attributnamens mit Trennzeichen, falls nötig
            attribute_to_search = convert_string_if_contains_capitals_or_spaces(attribute, db_dialect)
            # Nicht textbasierte Attribute werden in den dialektspezifischen Textdatentyp konvertiert.
            if table_meta_data.get_data_type_group(attribute) != 'text':
                attribute_to_search = f'CAST(sub.{attribute_to_search} AS {cast_data_type})'
            # Textbasierte Attribute werden ohne Typkonversion eingebunden.
            else:
                attribute_to_search = f'sub.{attribute_to_search}'
            # Die Bedingung für das erste Attribut wird unmittelbar an die bestehende Bedingung angehängt.
            if index == 0:
                condition = f"{condition} {attribute_to_search} {operator} {concat_string}"
            # Alle weiteren werden zusätzlich mit dem Operator OR versehen.
            else:
                condition = f"{condition} OR {attribute_to_search} {operator} {concat_string}"

    ## Abfragebedingung für die Vereinheitlichung ## 
    elif mode == 'unify':
        # In diesem Fall enthält die Liste der betroffenen Attribute nur einen Eintrag.
        affected_attribute = affected_attributes[0]
        # Datentypgruppe zur Überprüfung, ob Typkonversionen nötig sind
        data_type_group = table_meta_data.get_data_type_group(affected_attribute)
        ## Einfügen der zu vereinheitlichenden alten Werte in das Parameter-Dictionary ##
        for index, value in enumerate(old_values):
            ## Ist das betroffene Attribut eine Zahl oder ein Boolean-Wert, wird der Wert in den entsprechenden Python-Datentyp konvertiert. 
            # Hierdurch wird erreicht, dass SQLAlchemy Strings mit Anführungszeichen versieht, Zahlen jedoch nicht. ##
            if data_type_group == 'integer':
                condition_params['value_' + str(index)] = int(value)
            elif data_type_group == 'decimal':
                condition_params['value_' + str(index)] = float(value)
            elif data_type_group == 'boolean':
                condition_params['value_' + str(index)] = bool(value)
            # Für alle anderen Datentypgruppen wird der alte Wert unbearbeitet eingefügt.
            else:
                condition_params['value_' + str(index)] = value
            # Die Bedingung für das erste Attribut wird unmittelbar an die bestehende Bedingung angehängt.
            if index == 0:
                condition = f"{condition} sub.{convert_string_if_contains_capitals_or_spaces(affected_attribute, db_dialect)} = :{'value_' + str(index)}"
            # Alle weiteren werden zusätzlich mit dem Operator OR versehen.
            else:
                condition = f"{condition} OR sub.{convert_string_if_contains_capitals_or_spaces(affected_attribute, db_dialect)} = :{'value_' + str(index)}"
    # Zusammenfügen von UPDATE-Anweisung und Bedingung
    query = f'{query} {condition}'
    # In MariaDB muss die Abfrage mit der binären Version der Datenbankkollation ausgeführt werden, damit das Matching unter Berücksichtigung
    # von Groß- und Kleinschreibung erfolgt.
    if db_dialect == 'mariadb':
        query = f'{query} COLLATE {check_database_encoding(engine)}_bin'
    # Ausführen der Abfrage
    result = execute_sql_query(engine, text(query), condition_params)
    # Ausgabe des in eine Liste von Listen umgewandelten Ergebnisses ...
    if convert:
        return convert_result_to_list_of_lists(result)
    # ... oder des Ergebnisses als CursorResult.
    else:
        return result

def set_matching_operator_and_cast_data_type(db_dialect:str):
    """Festlegung des Operators und des Datentyps für die Typkonversion bei der Suche mittels regulärer Ausdrücke.
    
    db_dialect: Name des SQL-Dialekts der betroffenen Tabelle (aktuell 'mariadb' und 'postgresql' erlaubt)

    Ausgabe des Operators 'LIKE' und des Datentyps 'CHAR' für MariaDB; 'ILIKE' bzw. 'TEXT' für PostgreSQL."""

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
    """Erstellung des Strings für die Suche anhand regulärer Ausdrücke, wenn der Parameter aus Sicherheitsgründen mit bindparams in die Abfrage
    eingebunden wird.
    
    db_dialect: Name des SQL-Dialekts der betroffenen Datenbank (aktuell 'mariadb' und 'postgresql' erlaubt) 
    
    search_parameter_name: Name des Parameters, der mit bindparams an die Abfrage gebunden wird

    Ausgabe des zusammengefügten Strings; Ausgabe eines DialectErrors bei nicht unterstützten SQL-Dialekten."""

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
    """Escaping von Zeichen, die in regulären Ausdrücken in SQL eine besondere Funktion haben
    
    db_dialect: SQL-Dialekt der Datenbank (aktuell 'mariadb' oder 'postgresql')
    
    string: zu bearbeitender String
    
    Gibt den String entweder unbearbeitet oder mit Anpassung zurück."""
    
    # Prozentzeichen, Unterstriche, einfache Anführungszeichen und doppelte Anführungszeichen werden in regulären Ausdrücken in MariaDB und 
    # PostgreSQL jeweils durch Voranstellen eines Backslash als String (keine Zeichen mit besonderer Funktion) interpretiert.
    # In PostgreSQL müssen Backslashes verdoppelt werden, ...
    if db_dialect == 'postgresql':
        string = string.replace('\\', '\\\\').replace('%', '\%').replace('_', '\_').replace("'", "\'").replace('"', '\"')
    elif db_dialect == 'mariadb':
        # ... in MariaDB vervierfacht.
        string = string.replace('\\', '\\\\\\\\').replace('%', '\%').replace('_', '\_').replace("'", "\'").replace('"', '\"')
    return string