from argparse import ArgumentError
import sys
import pytest
from sqlalchemy import CursorResult, Engine, create_engine, text
import sqlalchemy
from ControllerClasses import TableMetaData
from model.SQLDatabaseError import DatabaseError, DialectError, QueryError
from model.databaseModel import build_sql_condition, get_data_type_meta_data, check_database_encoding, connect_to_db, convert_result_to_list_of_lists, convert_string_if_contains_capitals_or_spaces, execute_sql_query, get_full_table_ordered_by_primary_key, get_primary_key_from_engine, get_row_count_from_engine, list_all_tables_in_db_with_preview
import urllib.parse
# Anpassung der PATH-Variable, damit die Umgebungsvariablen aus environmentVariables.py eingelesen werden können
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

# Engine mit vom Aufbau her gültiger URL für SQLite, das bisher nicht unterstützt wird
@pytest.fixture
def fail_engine() -> Engine:
    return create_engine(f'sqlite:///C:/Benutzer/inexistent_database.db')

# TableMetaData-Objekt für die MariaDB-Tabelle Vorlesung_Datenbanken_SS2024
@pytest.fixture
def md_table_meta_data_1(maria_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2024'
    primary_keys = ['Matrikelnummer']
    data_type_info = {'Matrikelnummer': {'data_type_group': 'integer', 'data_type': 'int', 'numeric_precision': 10, 'is_nullable': False, 'column_default': None, 'is_unique': True, 'auto_increment': False}, 'Vorname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Nachname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}}
    row_count = 51
    return TableMetaData(maria_engine, table_name, primary_keys, data_type_info, row_count)

# TableMetaData-Objekt für die MariaDB-Tabelle Vorlesung_Datenbanken_SS2023
@pytest.fixture
def md_table_meta_data_2(maria_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2023'
    primary_keys = ['Matrikelnummer']
    data_type_info = {'Matrikelnummer': {'data_type_group': 'integer', 'data_type': 'int', 'numeric_precision': 10, 'is_nullable': False, 'column_default': None, 'is_unique': True, 'auto_increment': False}, 'Vorname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Nachname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'zugelassen': {'data_type_group': 'boolean', 'data_type': 'boolean', 'is_nullable': True, 'column_default': 0, 'is_unique': False, 'auto_increment': False}, 'Note': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 16, 'is_nullable': True, 'column_default': 'NULL', 'is_unique': False, 'auto_increment': False}}
    row_count = 11
    return TableMetaData(maria_engine, table_name, primary_keys, data_type_info, row_count)

# TableMetaData-Objekt für die PostgreSQL-Tabelle Vorlesung_Datenbanken_SS2024
@pytest.fixture
def pg_table_meta_data_1(postgres_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2024'
    primary_keys = ['Matrikelnummer']
    data_type_info = {'Matrikelnummer': {'data_type_group': 'integer', 'data_type': 'int', 'numeric_precision': 10, 'is_nullable': False, 'column_default': None, 'is_unique': True, 'auto_increment': False}, 'Vorname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Nachname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}}
    row_count = 51
    return TableMetaData(postgres_engine, table_name, primary_keys, data_type_info, row_count)

# TableMetaData-Objekt für die PostgreSQL-Tabelle Vorlesung_Datenbanken_SS2023
@pytest.fixture
def pg_table_meta_data_2(postgres_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2023'
    primary_keys = ['Matrikelnummer']
    data_type_info = {'Matrikelnummer': {'data_type_group': 'integer', 'data_type': 'integer', 'numeric_precision': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'zugelassen': {'data_type_group': 'boolean', 'data_type': 'boolean', 'is_unsigned': True, 'is_nullable': True, 'column_default': 'false', 'is_unique': False, 'auto_increment': False}, 'Vorname': {'data_type_group': 'text', 'data_type': 'character varying', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Nachname': {'data_type_group': 'text', 'data_type': 'character varying', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Note': {'data_type_group': 'text', 'data_type': 'character varying', 'character_max_length': 16, 'is_nullable': True, 'column_default': 'NULL::character varying', 'is_unique': False, 'auto_increment': False}}
    row_count = 11
    return TableMetaData(postgres_engine, table_name, primary_keys, data_type_info, row_count)

# TableMetaData-Objekt mit ungültigen Werten zum Testen der Fehlerausgaben
@pytest.fixture
def fail_table_meta_data(fail_engine: Engine) -> TableMetaData:
    table_name = 'IDontExist'
    primary_keys = ['KEY']
    data_type_info = {'KEY': {'data_type_group': 'nonsense', 'data_type': 'nonsense'}}
    row_count = 1000000
    return TableMetaData(fail_engine, table_name, primary_keys, data_type_info, row_count)


##### TESTS #####

### Tests der Funktionen, die Exceptions ausgeben können ###

# Test des Verbindungsaufbaus zu einer Datenbank
def test_connect_to_db() -> None:
    maria_engine = connect_to_db(ev.MARIADB_USERNAME, ev.MARIADB_PASSWORD, ev.MARIADB_SERVERNAME, ev.MARIADB_PORTNUMBER, 'MariaTest', 'mariadb', ev.MARIADB_ENCODING)
    postgres_engine = connect_to_db(ev.POSTGRES_USERNAME, ev.POSTGRES_PASSWORD, ev.POSTGRES_SERVERNAME, ev.POSTGRES_PORTNUMBER, 'PostgresTest1', 'postgresql', ev.POSTGRES_ENCODING)
    # Sicherstellung, dass das Ergebnis vom Typ Engine ist (nicht None)
    assert type(maria_engine) == Engine
    assert type(postgres_engine) == Engine
    # Sicherstellung, dass die Abfrage einer gültigen Tabelle ein gültiges Ergebnis liefert
    assert maria_engine.connect().execute(text('SELECT * FROM Vorlesung_Datenbanken_SS2024')) is not None
    assert postgres_engine.connect().execute(text('SELECT * FROM "Vorlesung_Datenbanken_SS2024"')) is not None

# Test der Fehlerausgaben bei falschen Eingaben für den Datenbankverbindungsaufbau
def test_connect_to_db_exceptions() -> None:
    # Überprüfung, ob connect_to_db einen DialectError ausgibt, wenn ein SQL-Dialekt verwendet wird, der nicht MariaDB oder PostgreSQL ist
    with pytest.raises(DialectError):
        connect_to_db(ev.MARIADB_USERNAME, ev.MARIADB_PASSWORD, ev.MARIADB_SERVERNAME, ev.MARIADB_PORTNUMBER, 'MariaTest', 'sqlite', ev.MARIADB_ENCODING)
    # Überprüfung, ob connect_to_db einen DatabaseError ausgibt, wenn ungültige Anmeldedaten übergeben werden
    with pytest.raises(DatabaseError):
        # Ungültiger Datenbankbenutzername
        connect_to_db('dfsdfsdf', ev.MARIADB_PASSWORD, ev.MARIADB_SERVERNAME, ev.MARIADB_PORTNUMBER, 'MariaTest', 'mariadb', ev.MARIADB_ENCODING)
        # Ungültiges Datenbankpasswort
        connect_to_db(ev.MARIADB_USERNAME, 'kl09ujlkjj', ev.MARIADB_SERVERNAME, ev.MARIADB_PORTNUMBER, 'MariaTest', 'mariadb', ev.MARIADB_ENCODING)
        # Ungültiger Datenbankservername
        connect_to_db(ev.MARIADB_USERNAME, ev.MARIADB_PASSWORD, 'google.com', ev.MARIADB_PORTNUMBER, 'MariaTest', 'mariadb', ev.MARIADB_ENCODING)
        # Ungültige Datenbankserverportnummer
        connect_to_db(ev.MARIADB_USERNAME, ev.MARIADB_PASSWORD, ev.MARIADB_SERVERNAME, 89632, 'MariaTest', 'mariadb', ev.MARIADB_ENCODING)
        # Ungültiger Datenbankname
        connect_to_db(ev.MARIADB_USERNAME, ev.MARIADB_PASSWORD, ev.MARIADB_SERVERNAME, ev.MARIADB_PORTNUMBER, 'sftrw', 'mariadb', ev.MARIADB_ENCODING)
        # Ungültige Zeichencodierung
        connect_to_db(ev.MARIADB_USERNAME, ev.MARIADB_PASSWORD, ev.MARIADB_SERVERNAME, ev.MARIADB_PORTNUMBER, 'MariaTest', 'mariadb', 'uft')
        
# Test der Erstellung der Vorschau für die Tabellenauswahl
def test_list_all_tables_in_db_with_preview(maria_engine: Engine, postgres_engine: Engine) -> None:
    # Tabellennamen, die im Ergebnis enthalten sein sollen
    maria_table_names = ['uebung_datenbanken_ss2024', 'vorlesung_datenbanken_ss2023', 'vorlesung_datenbanken_ss2024']
    postgres_table_names = ['Uebung_Datenbanken_SS2024', 'Vorlesung_Datenbanken_SS2023', 'Vorlesung_Datenbanken_SS2024']
    # Ausführung der Funktion
    maria_tables_and_columns, maria_previews, maria_tables_without_pks = list_all_tables_in_db_with_preview(maria_engine)
    postgres_tables_and_columns, postgres_previews, postgres_tables_without_pks = list_all_tables_in_db_with_preview(postgres_engine)
    # Überprüfung, dass alle Tabellennamen als Schlüssel im ersten Element des Ergebnistupels enthalten sind
    for table in maria_table_names:
        assert table in maria_tables_and_columns.keys()
    for table in postgres_table_names:
        assert table in postgres_tables_and_columns.keys()
    # Überprüfung, dass die Vorschau für jede Tabelle 20 Einträge enthält, wenn die Tabelle min. 20 Tupel hat
    assert len(maria_previews['uebung_datenbanken_ss2024']) == 20
    assert len(postgres_previews['Uebung_Datenbanken_SS2024']) == 20
    assert len(maria_previews['vorlesung_datenbanken_ss2024']) == 20
    assert len(postgres_previews['Vorlesung_Datenbanken_SS2024']) == 20
    # Überprüfung, dass die Vorschau für Tabellen mit weniger als 20 Einträgen die entsprechende Anzahl Einträge hat
    assert len(maria_previews['vorlesung_datenbanken_ss2023']) == 11
    assert len(postgres_previews['Vorlesung_Datenbanken_SS2023']) == 11
    # Überprüfung, dass die Anzahl der Schlüssel im Vorschau-Dictionary mit der Anzahl der abzufragenden Tabellen übereinstimmt
    assert len(maria_previews.keys()) == len(maria_table_names)
    assert len(postgres_previews.keys()) == len(postgres_table_names)
    # Überprüfung, dass die Tupel in jeder Vorschau für jedes Attribut der entsprechenden Tabelle einen Eintrag enthält
    for key in maria_previews.keys():
        for row in maria_previews[key]:
            assert len(row) == len(maria_tables_and_columns[key])
    for key in postgres_previews.keys():
        for row in postgres_previews[key]:
            assert len(row) == len(postgres_tables_and_columns[key])
    # Überprüfung, dass die Liste mit Tabellen ohne Primärschlüssel für beide Datenbanken leer ist
    assert maria_tables_without_pks == []
    assert postgres_tables_without_pks == []

# Test der Ausgabe eines DialectErrors bei der Vorschauerstellung, wenn die angegebene Engine einen nicht unterstützten SQL-Dialekt aufweist
def test_list_all_tables_in_db_with_preview_exception(fail_engine: Engine) -> None:
    with pytest.raises(DialectError):
        list_all_tables_in_db_with_preview(fail_engine)

# Test der Abfrage der vollen, nach Primärschlüsseln geordneten Tabelle
def test_get_full_table_ordered_by_primary_key(md_table_meta_data_2: TableMetaData, pg_table_meta_data_2: TableMetaData) -> None:
    # Überprüfung, dass das Ergebnis standardmäßig in eine Liste von Listen konvertiert wird ...
    converted_maria_result = get_full_table_ordered_by_primary_key(md_table_meta_data_2)
    converted_postgres_result = get_full_table_ordered_by_primary_key(pg_table_meta_data_2)
    assert type(converted_maria_result) == list
    assert type(converted_postgres_result) == list
    # ... bzw. das Abfrageergebnis bei Setzen des Parameters 'convert' auf False als CursorResult ausgegeben wird
    unconverted_maria_result = get_full_table_ordered_by_primary_key(md_table_meta_data_2, convert = False)
    unconverted_postgres_result = get_full_table_ordered_by_primary_key(pg_table_meta_data_2, convert = False)
    assert type(unconverted_maria_result) == CursorResult
    assert type(unconverted_postgres_result) == CursorResult
    # Da beide Tabellen denselben Aufbau haben, sollten die Abfrageergebnisse identisch sein.
    assert converted_maria_result == converted_postgres_result
    # Konkrete Überprüfung des Aufbaus
    assert converted_maria_result == [[1432209, 'Hendrik', 'Nielsen', 1, '1.0'], [1503456, 'Jessica', 'Wolnitz', 0, None], [2000675, 'Anton', 'Hegl', 0, None], [2111098, 'Zara', 'Lohefalter', 1, '4.0'], [2233449, 'Tatiana', 'Hatt', 0, None], [2340992, 'Carlos', 'Metzger', 1, '2.7'], [2345644, 'Tristan', 'Ingwersen', 1, '5.0'], [2356781, 'Benedikt', 'Friedrichs', 1, 'n.b.'], [2360099, 'Gustav', 'Grant', 1, 'n. b.'], [2398562, 'Karl', 'Heinz', 1, '2.7'], [2400563, 'Gudrun', 'Becker', 0, None]]
    assert converted_postgres_result == [[1432209, 'Hendrik', 'Nielsen', 1, '1.0'], [1503456, 'Jessica', 'Wolnitz', 0, None], [2000675, 'Anton', 'Hegl', 0, None], [2111098, 'Zara', 'Lohefalter', 1, '4.0'], [2233449, 'Tatiana', 'Hatt', 0, None], [2340992, 'Carlos', 'Metzger', 1, '2.7'], [2345644, 'Tristan', 'Ingwersen', 1, '5.0'], [2356781, 'Benedikt', 'Friedrichs', 1, 'n.b.'], [2360099, 'Gustav', 'Grant', 1, 'n. b.'], [2398562, 'Karl', 'Heinz', 1, '2.7'], [2400563, 'Gudrun', 'Becker', 0, None]]

# Test der Ausgabe eines DialectErrors bei der Abfrage, wenn die angegebene Engine einen nicht unterstützten SQL-Dialekt aufweist
def test_get_full_table_ordered_by_primary_key_exception(fail_table_meta_data: TableMetaData) -> None:
    with pytest.raises(DialectError):
        get_full_table_ordered_by_primary_key(fail_table_meta_data)

# Test der Abfrage der Gesamttupelanzahl einer Tabelle
def test_get_row_count_from_engine(maria_engine: Engine, postgres_engine: Engine) -> None:
    assert get_row_count_from_engine(maria_engine, 'Vorlesung_Datenbanken_SS2024') == 51
    assert get_row_count_from_engine(maria_engine, 'Vorlesung_Datenbanken_SS2023') == 11
    assert get_row_count_from_engine(postgres_engine, 'Vorlesung_Datenbanken_SS2024') == 51
    assert get_row_count_from_engine(postgres_engine, 'Vorlesung_Datenbanken_SS2023') == 11

# Test der Ausgabe eines DialectErrors bei der Abfrage, wenn die angegebene Engine einen nicht unterstützten SQL-Dialekt aufweist
def test_get_row_count_from_engine_exception(fail_engine: Engine) -> None:
    with pytest.raises(DialectError):
        get_row_count_from_engine(fail_engine, 'inexistent_table')

# Test der korrekten Formatierung der formulierten SQL-Bedingung
def test_build_sql_condition() -> None:
    assert build_sql_condition(('matrikelnummer', 'zugelassen'), 'postgresql', 'AND') == 'WHERE matrikelnummer = :matrikelnummer AND zugelassen = :zugelassen'
    # Sonderfall: In PostgreSQL müssen Tabellen- und Attributnamen mit Großbuchstaben in doppelte Anführungszeichen gesetzt werden, da sie sonst
    # kleingeschrieben interpretiert werden und zu Fehlern führen.
    assert build_sql_condition(('Matrikelnummer', 'zugelassen'), 'postgresql', 'AND') == 'WHERE "Matrikelnummer" = :Matrikelnummer AND zugelassen = :zugelassen'
    assert build_sql_condition(('matrikelnummer', 'zugelassen'), 'mariadb', 'AND') == 'WHERE matrikelnummer = :matrikelnummer AND zugelassen = :zugelassen'
    assert build_sql_condition(('Matrikelnummer', 'zugelassen'), 'mariadb', 'AND') == 'WHERE Matrikelnummer = :Matrikelnummer AND zugelassen = :zugelassen'
    # Wenn nur ein Attribut in die Bedingung eingebunden werden soll, ist kein Operator nötig. Der Attributname muss dennoch als Tupel übergeben werden.
    assert build_sql_condition(('Matrikelnummer',), 'postgresql') == 'WHERE "Matrikelnummer" = :Matrikelnummer'

# Test der Ausgabe eines QueryErrors, wenn bei mehr als einem angegebenen Attribut kein Operator genannt ist
def test_build_sql_condition_exception() -> None:    
    with pytest.raises(QueryError):
        build_sql_condition(('Matrikelnummer', 'Punktzahl'), 'postgresql')
        
# Test der Abfrage der Datenbank-Zeichencodierung
def test_check_database_encoding(maria_engine: Engine, postgres_engine: Engine) -> None:
    assert check_database_encoding(maria_engine) == 'utf8mb4'
    assert check_database_encoding(postgres_engine) == 'UTF8'

# Test der Ausgabe eines DialectErrors bei der Abfrage der Datenbank-Zeichencodierung, wenn die angegebene Engine einen nicht unterstützen 
# SQL-Dialekt aufweist
def test_check_database_encoding(fail_engine: Engine) -> None:
    with pytest.raises(DialectError):
        check_database_encoding(fail_engine)

# Test der Funktion zur Ausführung von SQL-Anweisungen
def test_execute_sql_query(md_table_meta_data_1: TableMetaData, pg_table_meta_data_1: TableMetaData) -> None:
    for data in [md_table_meta_data_1, pg_table_meta_data_1]:
        # Test-Abfrage des ersten Wertes für das erste Attribut in der Attributliste der jeweiligen Tabelle
        test_attribute = convert_string_if_contains_capitals_or_spaces(data.columns[0], data.engine.dialect.name)
        test_table_name = convert_string_if_contains_capitals_or_spaces(data.table_name, data.engine.dialect.name)
        query = text(f'SELECT {test_attribute} FROM {test_table_name} LIMIT 1')
        result = execute_sql_query(data.engine, query)
        # Überprüfung, dass hierbei ein gültiges Ergebnis vom Typ CursorResult erzeugt wird
        assert result is not None and type(result) == CursorResult

        # Test der Abfragenausführung mit Parametern anhand des Ergebnisses der vorigen Abfrage
        params = {'test_value': result.fetchone()[0]}
        param_result = execute_sql_query(data.engine, text(f'SELECT * FROM {test_table_name} WHERE {test_attribute} = :test_value'), params)
        # Das Ergebnis sollte nicht None sein ...
        assert param_result is not None 
        # ... und min. einen Wert enthalten.
        assert len(list(param_result)) > 0

# Test der Ausgabe von Exceptions bei falschen Argumenten für die Abfragenausführung
def test_execute_sql_query_exceptions(fail_table_meta_data: TableMetaData, postgres_engine: Engine) -> None:
    # Ausgabe eines ProgrammingErrors bei falscher Syntax, z. B. wenn eine nicht existierende Tabelle abgefragt werden soll
    with pytest.raises(sqlalchemy.exc.ProgrammingError):
        execute_sql_query(postgres_engine, text('SELECT * FROM inexistent_table'), None, raise_exceptions = True)
    # Beispiel für die Ausgabe sonstiger Exceptions für Fehler bei der Datenbankabfrage (z. B. nicht existierende Datenbank)
    with pytest.raises(Exception):
        execute_sql_query(fail_table_meta_data.engine, text('SELECT *'), None, raise_exceptions = True)
    
# Test der Abfrage der Primärschlüsselattribute aus den Servertabellen
def test_get_primary_key_from_engine(maria_engine: Engine, postgres_engine: Engine) -> None:
    # Sicherstellung, dass die Liste der Primärschlüssel in MariaDB und PostgreSQL für die Tabelle Vorlesung_Datenbanken_SS2024 jeweils nur das
    # Attribut Matrikelnummer enthält
    assert get_primary_key_from_engine(maria_engine, 'Vorlesung_Datenbanken_SS2024') == ['Matrikelnummer']
    assert get_primary_key_from_engine(postgres_engine, 'Vorlesung_Datenbanken_SS2024') == ['Matrikelnummer']

# Test der Ausgabe eines DialectErrors bei der Primärschlüsselabfrage, wenn die angegebene Engine einen nicht unterstützten SQL-Dialekt aufweist
def test_get_primary_key_from_engine_exceptions(fail_engine: Engine) -> None:
    with pytest.raises(DialectError):
        get_primary_key_from_engine(fail_engine, 'inexistent_table')

# Test der Abfrage von Datentypinformationen
def test_get_data_type_meta_data(maria_engine: Engine, postgres_engine: Engine) -> None:
    assert get_data_type_meta_data(maria_engine, 'Vorlesung_Datenbanken_SS2023') == {'Matrikelnummer': {'data_type_group': 'integer', 'data_type': 'int', 'numeric_precision': 10, 'is_nullable': False, 'column_default': None, 'is_unique': True, 'auto_increment': False}, 'Vorname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Nachname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'zugelassen': {'data_type_group': 'boolean', 'data_type': 'boolean', 'is_nullable': True, 'column_default': '0', 'is_unique': False, 'auto_increment': False}, 'Note': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 16, 'is_nullable': True, 'column_default': 'NULL', 'is_unique': False, 'auto_increment': False}}
    assert get_data_type_meta_data(postgres_engine, 'Vorlesung_Datenbanken_SS2023') == {'Matrikelnummer': {'data_type_group': 'integer', 'data_type': 'integer', 'numeric_precision': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'zugelassen': {'data_type_group': 'boolean', 'data_type': 'boolean', 'is_unsigned': True, 'is_nullable': True, 'column_default': 'false', 'is_unique': False, 'auto_increment': False}, 'Vorname': {'data_type_group': 'text', 'data_type': 'character varying', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Nachname': {'data_type_group': 'text', 'data_type': 'character varying', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Note': {'data_type_group': 'text', 'data_type': 'character varying', 'character_max_length': 16, 'is_nullable': True, 'column_default': 'NULL::character varying', 'is_unique': False, 'auto_increment': False}}

# Test der Ausgabe von Exceptions bei der Abfrage der Datentypinformationen, wenn ungültige Parameter angegeben wurden
def test_check_data_type_meta_data_exceptions(maria_engine: Engine, fail_engine: Engine) -> None:
    # Ausgabe eines DialectErrors bei nicht unterstütztem SQL-Dialekt
    with pytest.raises(DialectError):
        get_data_type_meta_data(fail_engine, 'inexistent_table')
    # Ausgabe eines ArgumentErrors ...
    with pytest.raises(ArgumentError):
        # ... wenn das erste Argument nicht vom Typ Engine ist
        get_data_type_meta_data('Helmut', 'Vorlesung_Datenbanken')
        # ... wenn das zweite Argument kein String ist und somit kein Tabellenname sein kann
        get_data_type_meta_data(maria_engine, 1111)


### Tests der Funktionen, die keine Exceptions ausgeben ###

# Test der Funktion für das Escaping von Tabellen- und Attributnamen
def test_convert_string_if_contains_capitals_or_spaces() -> None:
    pg = 'postgresql'
    mdb = 'mariadb'
    string_1 = 'vorige Punktzahl'
    string_2 = 'Matrikelnummer'
    string_3 = '"Vorname"'
    string_4 = 'anton'
    
  
    # In MariaDB werden nur Namen mit doppelten Anführungszeichen versehen, die Leerzeichen enthalten.
    assert convert_string_if_contains_capitals_or_spaces(string_1, mdb) == '"vorige Punktzahl"'
    # Alle anderen werden nicht verändert.
    assert convert_string_if_contains_capitals_or_spaces(string_2, mdb) == string_2
    # Ist der String schon von Anführungszeichen umgeben, werden keine weiteren hinzugefügt.
    assert convert_string_if_contains_capitals_or_spaces(string_3, mdb) == string_3
    assert convert_string_if_contains_capitals_or_spaces(string_4, mdb) == string_4

    # In PostgreSQL werden wie in MariaDB Namen mit doppelten Anführungszeichen versehen, die Leerzeichen enthalten.
    assert convert_string_if_contains_capitals_or_spaces(string_1, pg) == '"vorige Punktzahl"'
    # Außerdem werden Namen mit Großbuchstaben mit Anführungszeichen umgeben.
    assert convert_string_if_contains_capitals_or_spaces(string_2, pg) == '"Matrikelnummer"'
    # Ist der String schon von Anführungszeichen umgeben, werden keine weiteren hinzugefügt.
    assert convert_string_if_contains_capitals_or_spaces(string_3, pg) == string_3
    assert convert_string_if_contains_capitals_or_spaces(string_4, pg) == string_4
    
# Test der Funktion, die CursorResults (SQL-Abfrageergebnisse) in Listen von Listen umwandelt
def test_convert_result_to_list_of_lists(maria_engine: Engine, postgres_engine: Engine) -> None:
    # Beispielabfragen
    maria_result = maria_engine.connect().execute(text('SELECT Matrikelnummer FROM Vorlesung_Datenbanken_SS2023 LIMIT 5'))
    postgres_result = postgres_engine.connect().execute(text('SELECT "Matrikelnummer" FROM "Vorlesung_Datenbanken_SS2023" LIMIT 5'))
    # Umwandlung in Listen
    maria_list = convert_result_to_list_of_lists(maria_result)
    postgres_list = convert_result_to_list_of_lists(postgres_result)
    # Überprüfung, dass das Ergebnis tatsächlich eine Liste ist, ...
    assert type(maria_list) == list
    assert type(postgres_list) == list
    # ... die wie in der Abfrage angegeben 5 Tupel enthält, ...
    assert len(maria_list) == 5 
    assert len(postgres_list) == 5 
    # ... die jeweils einen Wert enthalten.
    for row in maria_list:
        assert len(row) == 1
    for row in postgres_list:
        assert len(row) == 1