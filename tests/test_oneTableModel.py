from argparse import ArgumentError
import sys
import pymysql
import pytest
from sqlalchemy import Engine, create_engine, text
import sqlalchemy
from ControllerClasses import TableMetaData
from model.SQLDatabaseError import DialectError, QueryError, UpdateError
from model.databaseModel import convert_result_to_list_of_lists, execute_sql_query, get_data_type_meta_data, get_primary_key_from_engine, get_row_count_from_engine
from model.oneTableModel import check_data_type_and_constraint_compatibility, escape_string, get_concatenated_string_for_matching, get_indexes_of_affected_attributes_for_replacing, get_replacement_information, get_row_number_of_affected_entries, get_unique_values_for_attribute, replace_all_string_occurrences, replace_some_string_occurrences, search_string, set_matching_operator_and_cast_data_type, update_to_unify_entries
import urllib.parse
# Anpassung der PATH-Variablen, damit die Umgebungsvariablen aus environmentVariables.py eingelesen werden können
sys.path.append('tests')
import environmentVariables as ev

### Festlegen der Fixtures, um sie in den Testfunktionen nutzen zu können, ohne sie mehrfach anzulegen ###
# MariaDB-Engine
@pytest.fixture
def maria_engine() -> Engine:
    return create_engine(f'mariadb+pymysql://{ev.MARIADB_USERNAME}:{urllib.parse.quote_plus(ev.MARIADB_PASSWORD)}@{ev.MARIADB_SERVERNAME}:{ev.MARIADB_PORTNUMBER}/MariaTest?charset=utf8mb4')

# PostgreSQL-Engine
@pytest.fixture
def postgres_engine() -> Engine:
    return create_engine(f'postgresql://{ev.POSTGRES_USERNAME}:{urllib.parse.quote_plus(ev.POSTGRES_PASSWORD)}@{ev.POSTGRES_SERVERNAME}:{ev.POSTGRES_PORTNUMBER}/PostgresTest1', connect_args = {'client_encoding': 'utf8'})

# TableMetaData-Objekt für die MariaDB-Tabelle Vorlesung_Datenbanken_SS2024
@pytest.fixture
def md_table_meta_data_1(maria_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2024'
    primary_keys = get_primary_key_from_engine(maria_engine, table_name)
    data_type_info = get_data_type_meta_data(maria_engine, table_name)
    row_count = get_row_count_from_engine(maria_engine, table_name)
    return TableMetaData(maria_engine, table_name, primary_keys, data_type_info, row_count)

# TableMetaData-Objekt für die MariaDB-Tabelle Vorlesung_Datenbanken_SS2023
@pytest.fixture
def md_table_meta_data_2(maria_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2023'
    primary_keys = get_primary_key_from_engine(maria_engine, table_name)
    data_type_info = get_data_type_meta_data(maria_engine, table_name)
    row_count = get_row_count_from_engine(maria_engine, table_name)
    return TableMetaData(maria_engine, table_name, primary_keys, data_type_info, row_count)

# TableMetaData-Objekt für die PostgreSQL-Tabelle Vorlesung_Datenbanken_SS2024
@pytest.fixture
def pg_table_meta_data_1(postgres_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2024'
    primary_keys = get_primary_key_from_engine(postgres_engine, table_name)
    data_type_info = get_data_type_meta_data(postgres_engine, table_name)
    row_count = get_row_count_from_engine(postgres_engine, table_name)
    return TableMetaData(postgres_engine, table_name, primary_keys, data_type_info, row_count)

# TableMetaData-Objekt für die PostgreSQL-Tabelle Vorlesung_Datenbanken_SS2023
@pytest.fixture
def pg_table_meta_data_2(postgres_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2023'
    primary_keys = get_primary_key_from_engine(postgres_engine, table_name)
    data_type_info = get_data_type_meta_data(postgres_engine, table_name)
    row_count = get_row_count_from_engine(postgres_engine, table_name)
    return TableMetaData(postgres_engine, table_name, primary_keys, data_type_info, row_count)


##### TESTS #####
    
# Überprüfung der Suchfunktion
def test_search_string(maria_engine:Engine, md_table_meta_data_1:TableMetaData, postgres_engine:Engine, pg_table_meta_data_1:TableMetaData) -> None:
    # Test für MariaDB
    maria_search_result = search_string(md_table_meta_data_1, 'Jo', ['Vorname', 'Nachname'])
    maria_row_count = maria_engine.connect().execute(text("SELECT COUNT(*) FROM Vorlesung_Datenbanken_SS2024 WHERE Vorname LIKE '%Jo%' OR Nachname LIKE '%Jo%'")).fetchone()[0]
    # Sicherstellung, dass alle 9 Tupel mit diesem Teilstring gefunden wurden
    assert maria_row_count == 9
    assert len(maria_search_result) == maria_row_count

    # Test für PostgreSQL
    postgres_search_result = search_string(pg_table_meta_data_1, 'Jo', ['Vorname', 'Nachname'])
    postgres_row_count = postgres_engine.connect().execute(text("SELECT COUNT(*) FROM \"Vorlesung_Datenbanken_SS2024\" WHERE \"Vorname\" ILIKE '%Jo%' OR \"Nachname\" ILIKE '%Jo%'")).fetchone()[0]
    # Sicherstellung, dass alle 9 Tupel mit diesem Teilstring gefunden wurden
    assert postgres_row_count == 9
    assert len(postgres_search_result) == postgres_row_count

### Tests für das Suchen und Ersetzen ###

# Überprüfung der Ermittlung der für die Anzeige der Ersetzungen nötigen Informationen
def test_get_replacement_information(md_table_meta_data_1: TableMetaData, pg_table_meta_data_1: TableMetaData) -> None:
    # Ersetzungsinformationen für MariaDB
    maria_row_nos_and_old_values, maria_occurrence_dict = get_replacement_information(md_table_meta_data_1, [('Matrikelnummer', 0), ('Vorname', 1), ('Nachname', 0)], 'Jo', 'Jojo')
    ### Sicherstellung, dass die beiden zurückgegebenen Dictionarys nicht None sind ###
    assert type(maria_row_nos_and_old_values) == dict
    assert type(maria_occurrence_dict) == dict
    ### Überprüfung des erwarteten Aufbaus ###
    assert maria_row_nos_and_old_values == {2: {'old': [1912967, 'Joanna', 'Hayes'], 'positions': [0, 1, 0], 'new': [None, 'Jojoanna', None], 'primary_key': [1912967]}, 19: {'old': [2695599, 'Joel', 'Turner'], 'positions': [0, 1, 0], 'new': [None, 'Jojoel', None], 'primary_key': [2695599]}, 24: {'old': [2838526, 'Joyce', 'Edwards'], 'positions': [0, 1, 0], 'new': [None, 'Jojoyce', None], 'primary_key': [2838526]}, 46: {'old': [4150993, 'Jonathan', 'Fox'], 'positions': [0, 1, 0], 'new': [None, 'Jojonathan', None], 'primary_key': [4150993]}, 48: {'old': [4490484, 'Joseph', 'Robinson'], 'positions': [0, 1, 0], 'new': [None, 'Jojoseph', None], 'primary_key': [4490484]}}
    assert maria_occurrence_dict == {1: {'row_no': 2, 'primary_key': [1912967], 'affected_attribute': 'Vorname'}, 2: {'row_no': 19, 'primary_key': [2695599], 'affected_attribute': 'Vorname'}, 3: {'row_no': 24, 'primary_key': [2838526], 'affected_attribute': 'Vorname'}, 4: {'row_no': 46, 'primary_key': [4150993], 'affected_attribute': 'Vorname'}, 5: {'row_no': 48, 'primary_key': [4490484], 'affected_attribute': 'Vorname'}}

    # Ersetzungsinformationen für PostgreSQL
    postgres_row_nos_and_old_values, postgres_occurrence_dict = get_replacement_information(pg_table_meta_data_1, [('Matrikelnummer', 0), ('Vorname', 1), ('Nachname', 0)], 'Jo', 'Jojo')
    ### Sicherstellung, dass die beiden zurückgegebenen Dictionarys nicht None sind ###
    assert type(postgres_row_nos_and_old_values) == dict
    assert type(postgres_occurrence_dict) == dict
    ### Überprüfung des erwarteten Aufbaus ###
    assert postgres_row_nos_and_old_values == {2: {'old': [1912967, 'Joanna', 'Hayes'], 'positions': [0, 1, 0], 'new': [None, 'Jojoanna', None], 'primary_key': [1912967]}, 19: {'old': [2695599, 'Joel', 'Turner'], 'positions': [0, 1, 0], 'new': [None, 'Jojoel', None], 'primary_key': [2695599]}, 24: {'old': [2838526, 'Joyce', 'Edwards'], 'positions': [0, 1, 0], 'new': [None, 'Jojoyce', None], 'primary_key': [2838526]}, 46: {'old': [4150993, 'Jonathan', 'Fox'], 'positions': [0, 1, 0], 'new': [None, 'Jojonathan', None], 'primary_key': [4150993]}, 48: {'old': [4490484, 'Joseph', 'Robinson'], 'positions': [0, 1, 0], 'new': [None, 'Jojoseph', None], 'primary_key': [4490484]}}
    assert postgres_occurrence_dict == {1: {'row_no': 2, 'primary_key': [1912967], 'affected_attribute': 'Vorname'}, 2: {'row_no': 19, 'primary_key': [2695599], 'affected_attribute': 'Vorname'}, 3: {'row_no': 24, 'primary_key': [2838526], 'affected_attribute': 'Vorname'}, 4: {'row_no': 46, 'primary_key': [4150993], 'affected_attribute': 'Vorname'}, 5: {'row_no': 48, 'primary_key': [4490484], 'affected_attribute': 'Vorname'}}

# Überprüfung der Ersetzung aller Vorkommen des gesuchten Strings
def test_replace_all_string_occurrences(md_table_meta_data_1: TableMetaData, md_table_meta_data_2: TableMetaData, pg_table_meta_data_1: TableMetaData, pg_table_meta_data_2: TableMetaData) -> None:
    ## Test, wenn nur ein Attribut durchsucht werden soll ##
    md_one_attribute_result = replace_all_string_occurrences(md_table_meta_data_1, ['Vorname'], 'Jo', 'Jojo')
    pg_one_attribute_result = replace_all_string_occurrences(pg_table_meta_data_1, ['Vorname'], 'Jo', 'Jojo')
    # Sicherstellung, dass nur die Einträge des durchsuchten Attributs ausgegeben werden
    assert md_one_attribute_result == [['Kevin'], ['Jojoanna'], ['Carla'], ['Lisa'], ['Renee'], ['Gregor'], ['Charles'], ['Steven'], ['Kaitlyn'], ['Anita'], ['Daniel'], ['Alicia'], ['Jim'], ['Anton'], ['Belinda'], ['Sarah'], ['Nicole'], ['Benjamin'], ['Jojoel'], ['Diana'], ['Siobhan'], ['Nancy'], ['Keith'], ['Jojoyce'], ['Katie'], ['Matthew'], ['Adam'], ['Angela'], ['Heather'], ['Kristin'], ['Bernard'], ['Ashley'], ['Jennifer'], ['Jason'], ['Carolyn'], ['Alexandra'], ['Cristina'], ['Denise'], ['Barbara'], ['Waltraud'], ['Angela'], ['Jeffrey'], ['Andrea'], ['Cindy'], ['Lauren'], ['Jojonathan'], ['Amanda'], ['Jojoseph'], ['Jay'], ['Denise'], ['Gloria']]
    assert pg_one_attribute_result == [['Kevin'], ['Jojoanna'], ['Carla'], ['Lisa'], ['Renee'], ['Gregor'], ['Charles'], ['Steven'], ['Kaitlyn'], ['Anita'], ['Daniel'], ['Alicia'], ['Jim'], ['Anton'], ['Belinda'], ['Sarah'], ['Nicole'], ['Benjamin'], ['Jojoel'], ['Diana'], ['Siobhan'], ['Nancy'], ['Keith'], ['Jojoyce'], ['Katie'], ['Matthew'], ['Adam'], ['Angela'], ['Heather'], ['Kristin'], ['Bernard'], ['Ashley'], ['Jennifer'], ['Jason'], ['Carolyn'], ['Alexandra'], ['Cristina'], ['Denise'], ['Barbara'], ['Waltraud'], ['Angela'], ['Jeffrey'], ['Andrea'], ['Cindy'], ['Lauren'], ['Jojonathan'], ['Amanda'], ['Jojoseph'], ['Jay'], ['Denise'], ['Gloria']]

    ## Test, wenn zwei Attribute durchsucht werden sollen ##
    md_two_attributes_result = replace_all_string_occurrences(md_table_meta_data_2, ['Vorname', 'Nachname'], 'an', 'AHN')
    pg_two_attributes_result = replace_all_string_occurrences(pg_table_meta_data_2, ['Vorname', 'Nachname'], 'an', 'AHN')
    # Sicherstellung, dass die Einträge aller Attribute ausgegeben werden
    assert md_two_attributes_result == [[1432209, 'Hendrik', 'Nielsen', 1, '1.0'], [1503456, 'Jessica', 'Wolnitz', 0, None], [2000675, 'Anton', 'Hegl', 0, None], [2111098, 'Zara', 'Lohefalter', 1, '4.0'], [2233449, 'TatiAHNa', 'Hatt', 0, None], [2340992, 'Carlos', 'Metzger', 1, '2.7'], [2345644, 'TristAHN', 'Ingwersen', 1, '5.0'], [2356781, 'Benedikt', 'Friedrichs', 1, 'n.b.'], [2360099, 'Gustav', 'GrAHNt', 1, 'n. b.'], [2398562, 'Karl', 'Heinz', 1, '2.7'], [2400563, 'Gudrun', 'Becker', 0, None]]
    assert pg_two_attributes_result == [[1432209, 'Hendrik', 'Nielsen', 1, '1.0'], [1503456, 'Jessica', 'Wolnitz', 0, None], [2000675, 'Anton', 'Hegl', 0, None], [2111098, 'Zara', 'Lohefalter', 1, '4.0'], [2233449, 'TatiAHNa', 'Hatt', 0, None], [2340992, 'Carlos', 'Metzger', 1, '2.7'], [2345644, 'TristAHN', 'Ingwersen', 1, '5.0'], [2356781, 'Benedikt', 'Friedrichs', 1, 'n.b.'], [2360099, 'Gustav', 'GrAHNt', 1, 'n. b.'], [2398562, 'Karl', 'Heinz', 1, '2.7'], [2400563, 'Gudrun', 'Becker', 0, None]]

# Test, ob bei der Ersetzung aller String-Vorkommen ein UpdateError ausgegeben wird, wenn die Abfrageparameter ungültig sind
def test_replace_all_string_occurrences_exception(md_table_meta_data_1: TableMetaData) -> None:
    # Abfrage eines Attributs, das in der Tabelle Vorlesung_Datenbanken_SS2024 nicht existiert
    with pytest.raises(UpdateError):
        replace_all_string_occurrences(md_table_meta_data_1, ['zugelassen'], 'ja', 'nein')

# Test der Ermittlung der Zeilen- und Positionsnummern der betroffenen Attribute
def test_get_indexes_of_affected_attributes_for_replacing(md_table_meta_data_1: TableMetaData, pg_table_meta_data_1: TableMetaData) -> None:
    ### Positionsinformationen für die Ersetzung eines Strings in einem Attribut ###
    md_indexes = get_indexes_of_affected_attributes_for_replacing(md_table_meta_data_1, 'Jo', ['Vorname'])
    pg_indexes = get_indexes_of_affected_attributes_for_replacing(pg_table_meta_data_1, 'Jo', ['Vorname'])
    ### Überprüfung, dass die erwarteten Zeilennummern als Schlüssel angegeben sind und die Positionsliste für alle Schlüssel gleich ist ###
    for key, value in md_indexes.items():
        assert key in [2, 19, 24, 46, 48]
        assert value == [0, 1, 0]
    for key, value in pg_indexes.items():
        assert key in [2, 19, 24, 46, 48]
        assert value == [0, 1, 0]

    ### Positionsinformationen für die Ersetzung eines Strings in zwei Attributen ###
    md_multiple_attribute_indexes = get_indexes_of_affected_attributes_for_replacing(md_table_meta_data_1, 'Jo', ['Vorname', 'Nachname'])
    pg_multiple_attribute_indexes = get_indexes_of_affected_attributes_for_replacing(pg_table_meta_data_1, 'Jo', ['Vorname', 'Nachname'])
    ### Sicherstellung, dass die Dictionarys die erwartete Form haben
    assert md_multiple_attribute_indexes == {2: [0, 1, 0], 3: [0, 0, 1], 4: [0, 0, 1], 19: [0, 1, 0], 24: [0, 1, 0], 27: [0, 0, 1], 43: [0, 0, 1], 46: [0, 1, 0], 48: [0, 1, 0]}
    assert pg_multiple_attribute_indexes == {2: [0, 1, 0], 3: [0, 0, 1], 4: [0, 0, 1], 19: [0, 1, 0], 24: [0, 1, 0], 27: [0, 0, 1], 43: [0, 0, 1], 46: [0, 1, 0], 48: [0, 1, 0]}

# Test der Ersetzung ausgewählter Vorkommen
def test_replace_some_string_occurrences(md_table_meta_data_2, pg_table_meta_data_2) -> None:
    # Ersetzung eines Vorkommens
    md_replace = replace_some_string_occurrences(md_table_meta_data_2, {0: {'primary_keys': ['Matrikelnummer']}, 1: {'row_no': 5, 'primary_key': [2233449], 'affected_attribute': 'Vorname'}}, 'an', 'AHN')
    pg_replace = replace_some_string_occurrences(pg_table_meta_data_2, {0: {'primary_keys': ['Matrikelnummer']}, 1: {'row_no': 5, 'primary_key': [2233449], 'affected_attribute': 'Vorname'}}, 'an', 'AHN')
    # Überprüfung, dass eine Erfolgsmeldung ausgegeben wird
    assert md_replace == 'Der ausgewählte Wert wurde erfolgreich aktualisiert.'
    assert pg_replace == 'Der ausgewählte Wert wurde erfolgreich aktualisiert.'

    # Ersetzung von zwei Vorkommen
    md_replace_two = replace_some_string_occurrences(md_table_meta_data_2, {0: {'primary_keys': ['Matrikelnummer']}, 1: {'row_no': 5, 'primary_key': [2233449], 'affected_attribute': 'Vorname'}, 2: {'row_no': 7, 'primary_key': [2345644], 'affected_attribute': 'Vorname'}}, 'an', 'AHN')
    pg_replace_two = replace_some_string_occurrences(pg_table_meta_data_2, {0: {'primary_keys': ['Matrikelnummer']}, 1: {'row_no': 5, 'primary_key': [2233449], 'affected_attribute': 'Vorname'}, 2: {'row_no': 7, 'primary_key': [2345644], 'affected_attribute': 'Vorname'}}, 'an', 'AHN')
    assert md_replace_two == 'Alle 2 ausgewählten Werte wurden erfolgreich aktualisiert.'
    assert pg_replace_two == 'Alle 2 ausgewählten Werte wurden erfolgreich aktualisiert.'

### Tests für das Vereinheitlichen von Datenbankeinträgen ###

def test_get_unique_values_for_attribute(md_table_meta_data_2: TableMetaData, pg_table_meta_data_2: TableMetaData) -> None:
    # Zurücksetzen der Tabellen auf den ursprünglichen Zustand; in MariaDB auf zwei Abfragen aufgeteilt
    execute_sql_query(md_table_meta_data_2.engine, text("DELETE FROM Vorlesung_Datenbanken_SS2023"), commit = True)
    execute_sql_query(md_table_meta_data_2.engine, text("INSERT INTO Vorlesung_Datenbanken_SS2023 (Matrikelnummer, Vorname, Nachname, zugelassen, Note) VALUES (1432209, 'Hendrik', 'Nielsen', TRUE, '1.0'), (1503456, 'Jessica', 'Wolnitz', FALSE, NULL), (2000675, 'Anton', 'Hegl', FALSE, NULL), (2111098, 'Zara', 'Lohefalter', TRUE, '4.0'), (2233449, 'Tatiana', 'Hatt', FALSE, NULL), (2340992, 'Carlos', 'Metzger', TRUE, '2.7'), (2345644, 'Tristan', 'Ingwersen', TRUE, '5.0'), (2356781, 'Benedikt', 'Friedrichs', TRUE, 'n.b.'), (2360099, 'Gustav', 'Grant', TRUE, 'n. b.'), (2398562, 'Karl', 'Heinz', TRUE, '2.7'), (2400563, 'Gudrun', 'Becker', FALSE, NULL)"), raise_exceptions=True, commit = True)
    execute_sql_query(pg_table_meta_data_2.engine, text("DELETE FROM \"Vorlesung_Datenbanken_SS2023\"; INSERT INTO \"Vorlesung_Datenbanken_SS2023\" (\"Matrikelnummer\", \"Vorname\", \"Nachname\", zugelassen, \"Note\") VALUES (1432209, 'Hendrik', 'Nielsen', TRUE, '1.0'), (1503456, 'Jessica', 'Wolnitz', FALSE, NULL), (2000675, 'Anton', 'Hegl', FALSE, NULL), (2111098, 'Zara', 'Lohefalter', TRUE, '4.0'), (2233449, 'Tatiana', 'Hatt', FALSE, NULL), (2340992, 'Carlos', 'Metzger', TRUE, '2.7'), (2345644, 'Tristan', 'Ingwersen', TRUE, '5.0'), (2356781, 'Benedikt', 'Friedrichs', TRUE, 'n.b.'), (2360099, 'Gustav', 'Grant', TRUE, 'n. b.'), (2398562, 'Karl', 'Heinz', TRUE, '2.7'), (2400563, 'Gudrun', 'Becker', FALSE, NULL)"), commit = True)
    # Liste der Werte, die das Attribut Note in beiden Tabellen enthalten sollte
    check_values = [None, '1.0', '2.7', '4.0', '5.0', 'n.b.', 'n. b.']
    ### Abfrage der einzigartigen Werte in den Tabellen mit dem Namen Vorlesung_Datenbanken_SS2023 in der MariaDB- und der PostgreSQL-Datenbank ###
    md_result = get_unique_values_for_attribute(md_table_meta_data_2, 'Note')
    pg_result = get_unique_values_for_attribute(pg_table_meta_data_2, 'Note')
    ### Umwandlung des Ergebnisses in eine Liste der einzigartigen Werte (d. h. Ignorieren des Zählers, wie oft die Tabelle den jeweiligen Wert
    # enthält) ###
    md_unique_values = []
    pg_unique_values = []
    for row in md_result:
        if row[0] not in md_unique_values:
            md_unique_values.append(row[0])  
    for row in pg_result:
        if row[0] not in pg_unique_values:
            pg_unique_values.append(row[0])
    ### Überprüfung, dass die erwarteten Werte in beiden Listen der tatsächlichen Werte enthalten sind ###
    for value in check_values:
        assert value in md_unique_values
        assert value in pg_unique_values

# Überprüfung der Vereinheitlichungsfunktion
def test_update_to_unify_entries(md_table_meta_data_2: TableMetaData, pg_table_meta_data_2: TableMetaData) -> None:
    # Vereinheitlichen der Werte '5.0', 'n.b.' und 'n. b.' im Attribut Note der Tabelle Vorlesung_Datenbanken_SS2023 zu 'n. b.'
    update_to_unify_entries(md_table_meta_data_2, 'Note', ['5.0', 'n.b.', 'n. b.'], 'n. b.', commit = True)
    update_to_unify_entries(pg_table_meta_data_2, 'Note', ['5.0', 'n.b.', 'n. b.'], 'n. b.', commit = True)
    # Abfrage der Tupel in den aktualisierten Tabellen, die die zu überschreibenden Werte enthalten
    md_check_result = convert_result_to_list_of_lists(execute_sql_query(md_table_meta_data_2.engine, text("SELECT Note FROM Vorlesung_Datenbanken_SS2023 WHERE Note = '5.0' OR Note = 'n.b.'")))
    pg_check_result = convert_result_to_list_of_lists(execute_sql_query(pg_table_meta_data_2.engine, text("SELECT \"Note\" FROM \"Vorlesung_Datenbanken_SS2023\" WHERE \"Note\" = '5.0' OR \"Note\" = 'n.b.'")))
    # In beiden Fällen sollte das Ergebnis leer sein.
    assert len(md_check_result) == 0
    assert len(pg_check_result) == 0
    ### Zurücksetzen der vereinheitlichten Werte, um die Tests mehrfach ausführen zu können ###
    # Muss in MariaDB in zwei Abfragen erfolgen, sonst wird eine Fehlermeldung ausgegeben
    execute_sql_query(md_table_meta_data_2.engine, text("DELETE FROM Vorlesung_Datenbanken_SS2023"), commit = True)
    execute_sql_query(md_table_meta_data_2.engine, text("INSERT INTO Vorlesung_Datenbanken_SS2023 (Matrikelnummer, Vorname, Nachname, zugelassen, Note) VALUES (1432209, 'Hendrik', 'Nielsen', TRUE, '1.0'), (1503456, 'Jessica', 'Wolnitz', FALSE, NULL), (2000675, 'Anton', 'Hegl', FALSE, NULL), (2111098, 'Zara', 'Lohefalter', TRUE, '4.0'), (2233449, 'Tatiana', 'Hatt', FALSE, NULL), (2340992, 'Carlos', 'Metzger', TRUE, '2.7'), (2345644, 'Tristan', 'Ingwersen', TRUE, '5.0'), (2356781, 'Benedikt', 'Friedrichs', TRUE, 'n.b.'), (2360099, 'Gustav', 'Grant', TRUE, 'n. b.'), (2398562, 'Karl', 'Heinz', TRUE, '2.7'), (2400563, 'Gudrun', 'Becker', FALSE, NULL)"), raise_exceptions=True, commit = True)
    execute_sql_query(pg_table_meta_data_2.engine, text("DELETE FROM \"Vorlesung_Datenbanken_SS2023\"; INSERT INTO \"Vorlesung_Datenbanken_SS2023\" (\"Matrikelnummer\", \"Vorname\", \"Nachname\", zugelassen, \"Note\") VALUES (1432209, 'Hendrik', 'Nielsen', TRUE, '1.0'), (1503456, 'Jessica', 'Wolnitz', FALSE, NULL), (2000675, 'Anton', 'Hegl', FALSE, NULL), (2111098, 'Zara', 'Lohefalter', TRUE, '4.0'), (2233449, 'Tatiana', 'Hatt', FALSE, NULL), (2340992, 'Carlos', 'Metzger', TRUE, '2.7'), (2345644, 'Tristan', 'Ingwersen', TRUE, '5.0'), (2356781, 'Benedikt', 'Friedrichs', TRUE, 'n.b.'), (2360099, 'Gustav', 'Grant', TRUE, 'n. b.'), (2398562, 'Karl', 'Heinz', TRUE, '2.7'), (2400563, 'Gudrun', 'Becker', FALSE, NULL)"), commit = True)

### Tests für die Hilfsfunktionen ###

# Test der Überprüfung der Kompatibilität des neu einzusetzenden Wertes mit bestehenden Constraints des Attributs
def test_check_data_type_and_constraint_compatibility(md_table_meta_data_2: TableMetaData, pg_table_meta_data_2: TableMetaData) -> None:
    # Test für das Einfügen eines ganzzahligen Wertes in ein ganzzahliges Attribut
    assert check_data_type_and_constraint_compatibility(md_table_meta_data_2, 'Matrikelnummer', 1432210, 1432209) == 0
    assert check_data_type_and_constraint_compatibility(pg_table_meta_data_2, 'Matrikelnummer', 1432210, 1432209) == 0
   

# Überprüfung der Ausgabe von Exceptions bei der Kompatibilitätsprüfung
def test_check_data_type_and_constraint_compatibility_exceptions(md_table_meta_data_2: TableMetaData, pg_table_meta_data_2: TableMetaData) -> None:
    # Test, dass beim Einfügen eines Wertes mit dem korrekten Datentyp, ohne dass der angegebene alte Wert in der Tabelle enthalten ist, ein
    # QueryError ausgegeben wird
    with pytest.raises(QueryError):
        check_data_type_and_constraint_compatibility(md_table_meta_data_2, 'Matrikelnummer', 1432210, 1432211)
        check_data_type_and_constraint_compatibility(pg_table_meta_data_2, 'Matrikelnummer', 1432210, 1432211)

    # Test, ob check_data_type_and_constraint_compatibility bei der Eintragung nicht textbasierter Werte mit regexp_replace einen sqlalchemy.exc.ProgrammingError
    # ausgibt  
    with pytest.raises(sqlalchemy.exc.ProgrammingError):
        assert check_data_type_and_constraint_compatibility(md_table_meta_data_2, 'Note', False, '1.0') == 0
        assert check_data_type_and_constraint_compatibility(pg_table_meta_data_2, 'Note', False, '1.0') == 0  

    ### Überprüfung der Ausgabe von Exceptions bei ungeeigneten Argumenten oder Constraint-Verletzungen in 
    # check_data_type_and_constraint_compatibility ###
    # Als Input und als alter Wert dürfen nur Strings, ganze Zahlen, Boolean-Werte oder Dezimalzahlen angegeben werden. 
    with pytest.raises(ArgumentError):
        check_data_type_and_constraint_compatibility(md_table_meta_data_2, 'Matrikelnummer', pg_table_meta_data_2, 1432209)
        check_data_type_and_constraint_compatibility(pg_table_meta_data_2, 'Matrikelnummer', md_table_meta_data_2, 1432209)
        check_data_type_and_constraint_compatibility(md_table_meta_data_2, 'Matrikelnummer', 'MatrNr', pg_table_meta_data_2)
        check_data_type_and_constraint_compatibility(pg_table_meta_data_2, 'Matrikelnummer', 'MatrNr', md_table_meta_data_2)

    # Das Attribut Matrikelnummer weist eine CHECK-Constraint auf, mit der überprüft wird, dass die eingegebenen Werte genau 7 Ziffern enthalten.
    # Wird diese verletzt, tritt ein sqlalchemy.exc.OperationalError auf.
    with pytest.raises(sqlalchemy.exc.OperationalError):
        check_data_type_and_constraint_compatibility(md_table_meta_data_2, 'Matrikelnummer', 14523333333333, 1432209)
        check_data_type_and_constraint_compatibility(pg_table_meta_data_2, 'Matrikelnummer', 50000000000, 1432209)
    
# Überprüfung der Ermittlung der Zeilennumern und der alten Werte der von der Änderung betroffenen Tupel
def test_get_row_number_of_affected_entries(md_table_meta_data_1: TableMetaData, pg_table_meta_data_1: TableMetaData, md_table_meta_data_2: TableMetaData, pg_table_meta_data_2: TableMetaData) -> None:
    ### Test für den Ersetzungsmodus ###
    md_replace_rows = get_row_number_of_affected_entries(md_table_meta_data_1, ['Vorname'], ['Jo'], mode = 'replace')
    pg_replace_rows = get_row_number_of_affected_entries(pg_table_meta_data_1, ['Vorname'], ['Jo'], mode = 'replace')
    # Überprüfung, dass die Ergebnisse in beiden Dialekten die erwartete Form haben
    assert md_replace_rows == [[2, 1912967, 'Joanna', 'Hayes'], [19, 2695599, 'Joel', 'Turner'], [24, 2838526, 'Joyce', 'Edwards'], [46, 4150993, 'Jonathan', 'Fox'], [48, 4490484, 'Joseph', 'Robinson']]
    assert pg_replace_rows == [[2, 1912967, 'Joanna', 'Hayes'], [19, 2695599, 'Joel', 'Turner'], [24, 2838526, 'Joyce', 'Edwards'], [46, 4150993, 'Jonathan', 'Fox'], [48, 4490484, 'Joseph', 'Robinson']]
    
    ### Test für den Vereinheitlichungsmodus ###
    md_unify_rows = get_row_number_of_affected_entries(md_table_meta_data_2, ['Note'], ['5.0', 'n.b.', 'n. b.'], mode = 'unify')
    pg_unify_rows = get_row_number_of_affected_entries(pg_table_meta_data_2, ['Note'], ['5.0', 'n.b.', 'n. b.'], mode = 'unify')
    # Überprüfung, dass die Ergebnisse in beiden Dialekten die erwartete Form haben
    assert md_unify_rows == [[7, 2345644, 'Tristan', 'Ingwersen', 1, '5.0'], [8, 2356781, 'Benedikt', 'Friedrichs', 1, 'n.b.'], [9, 2360099, 'Gustav', 'Grant', 1, 'n. b.']]
    assert pg_unify_rows == [[7, 2345644, 'Tristan', 'Ingwersen', 1, '5.0'], [8, 2356781, 'Benedikt', 'Friedrichs', 1, 'n.b.'], [9, 2360099, 'Gustav', 'Grant', 1, 'n. b.']]

# Überprüfung, dass bei falschen Argumenten für die Ermittlung der betroffenen Tupel ein ArgumentError ausgegeben wird
def test_get_row_number_of_affected_entries_exceptions(md_table_meta_data_1: TableMetaData, md_table_meta_data_2: TableMetaData) -> None:
    with pytest.raises(ArgumentError):
        # mehr als ein zu betrachtender Wert für den Ersetzungsmodus angegeben
        get_row_number_of_affected_entries(md_table_meta_data_1, ['Vorname'], ['Jo', 'Ann'], mode = 'replace')
        # mehr als ein zu betrachtendes Attribut für den Vereinheitlichungsmodus angegeben
        get_row_number_of_affected_entries(md_table_meta_data_2, ['Note', 'zugelassen'], ['5.0', 'n.b.', 'n. b.'], mode = 'unify')
        # ungültiger Modus
        get_row_number_of_affected_entries(md_table_meta_data_1, ['Vorname'], ['Jo'], mode = 'merge')

# Überprüfung der Funktion für die Ausgabe des dialektspezifischen Operators und des Datentyps für das String-Matching
def test_set_matching_operator_and_cast_data_type() -> None:
    # In PostgreSQL muss der Operator ILIKE verwendet werden, um Groß- und Kleinschreibung zu ignorieren. Der Konversionsdatentyp ist TEXT.
    assert set_matching_operator_and_cast_data_type('postgresql') == ('ILIKE', 'TEXT')
    # In MariaDB wird Groß- und Kleinschreibung mit dem Operator LIKE standardmäßig ignoriert. Der Konversionsdatentyp ist CHAR.
    assert set_matching_operator_and_cast_data_type('mariadb') == ('LIKE', 'CHAR')

# Überprüfung der Funktion für den Aufbau des Suchstrings bei der Verwendung regulärer Ausdrücke
def test_get_concatenated_string_for_matching() -> None:
    # In PostgreSQL ist der Operator || zu verwenden.
    assert get_concatenated_string_for_matching('postgresql', 'Datenbanken') == "'%' || :Datenbanken || '%'"
    # In MariaDB werden Strings hingegen mit der Funktion CONCAT() zusammengefügt.
    assert get_concatenated_string_for_matching('mariadb', 'Datenbanken') == "CONCAT('%', CONCAT(:Datenbanken, '%'))"

 # Überprüfung, dass Steuerzeichen für reguläre Ausdrücke dialektspezifisch mit Escape-Zeichen versehen werden:
 # %, _ und ' jeweils mit einem Backslash
 # / wird in PostgreSQL verdoppelt, in MariaDB vervierfacht.
def test_escape_string() -> None:
    ### In PostgreSQL werden Prozentzeichen, Unterstriche und doppelte Anführungszeichen mit einem Backslash versehen ###
    assert escape_string('postgresql', '25%') == '25\%'
    assert escape_string('postgresql', 'uebung_datenbanken') == 'uebung\_datenbanken'
    assert escape_string('postgresql', "O'Brian") == "O\'Brian"
    assert escape_string('postgresql', '"anfangs"') == '\"anfangs\"'
    # Im String enthaltene Backslashes werden verdoppelt.
    assert escape_string('postgresql', 'http:\\') == 'http:\\\\'

    ### In MariaDB werden Prozentzeichen, Unterstriche und doppelte Anführungszeichen mit einem Backslash versehen ###
    assert escape_string('mariadb', '25%') == '25\%'
    assert escape_string('mariadb', 'uebung_datenbanken') == 'uebung\_datenbanken'
    assert escape_string('mariadb', "O'Brian") == "O\'Brian"
    assert escape_string('postgresql', '"anfangs"') == '\"anfangs\"'
    # Im String enthaltene Backslashes werden vervierfacht.    
    assert escape_string('mariadb', 'http:\\') == 'http:\\\\\\\\'

