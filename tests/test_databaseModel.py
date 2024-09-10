from argparse import ArgumentError
import sys
import pytest
from sqlalchemy import CursorResult, Engine, create_engine, text
import sqlalchemy
from ControllerClasses import TableMetaData
from model.SQLDatabaseError import DatabaseError, DialectError, QueryError
from model.databaseModel import build_sql_condition, get_data_type_meta_data, check_database_encoding, connect_to_db, convert_result_to_list_of_lists, convert_string_if_contains_capitals_or_spaces, execute_sql_query, get_full_table_ordered_by_primary_key, get_primary_key_from_engine, get_row_count_from_engine, list_all_tables_in_db_with_preview
import urllib.parse
sys.path.append('tests')
import environmentVariables as ev


### Festlegen der Fixtures, um sie in den Testfunktionen nutzen zu können, ohne sie mehrfach anzulegen ###
@pytest.fixture
def maria_engine() -> Engine:
    return create_engine(f'mariadb+pymysql://{ev.MARIADB_USERNAME}:{urllib.parse.quote_plus(ev.MARIADB_PASSWORD)}@{ev.MARIADB_SERVERNAME}:{ev.MARIADB_PORTNUMBER}/MariaTest?charset=utf8mb4')

@pytest.fixture
def postgres_engine() -> Engine:
    return create_engine(f'postgresql://{ev.POSTGRES_USERNAME}:{urllib.parse.quote_plus(ev.POSTGRES_PASSWORD)}@{ev.POSTGRES_SERVERNAME}:{ev.POSTGRES_PORTNUMBER}/PostgresTest1', connect_args = {'client_encoding': 'utf8'})

@pytest.fixture
def fail_engine() -> Engine:
    return create_engine(f'sqlite:///C:/Benutzer/inexistent_database.db')

@pytest.fixture
def md_table_meta_data_1(maria_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2024'
    primary_keys = ['Matrikelnummer']
    data_type_info = {'Matrikelnummer': {'data_type_group': 'integer', 'data_type': 'int', 'numeric_precision': 10, 'is_nullable': False, 'column_default': None, 'is_unique': True, 'auto_increment': False}, 'Vorname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Nachname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}}
    row_count = 51
    return TableMetaData(maria_engine, table_name, primary_keys, data_type_info, row_count)

@pytest.fixture
def md_table_meta_data_2(maria_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2023'
    primary_keys = ['Matrikelnummer']
    data_type_info = {'Matrikelnummer': {'data_type_group': 'integer', 'data_type': 'int', 'numeric_precision': 10, 'is_nullable': False, 'column_default': None, 'is_unique': True, 'auto_increment': False}, 'Vorname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Nachname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'zugelassen': {'data_type_group': 'boolean', 'data_type': 'boolean', 'is_nullable': True, 'column_default': 0, 'is_unique': False, 'auto_increment': False}, 'Note': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 16, 'is_nullable': True, 'column_default': 'NULL', 'is_unique': False, 'auto_increment': False}}
    row_count = 11
    return TableMetaData(maria_engine, table_name, primary_keys, data_type_info, row_count)

@pytest.fixture
def pg_table_meta_data_1(postgres_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2024'
    primary_keys = ['Matrikelnummer']
    data_type_info = {'Matrikelnummer': {'data_type_group': 'integer', 'data_type': 'int', 'numeric_precision': 10, 'is_nullable': False, 'column_default': None, 'is_unique': True, 'auto_increment': False}, 'Vorname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Nachname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}}
    row_count = 51
    return TableMetaData(postgres_engine, table_name, primary_keys, data_type_info, row_count)

@pytest.fixture
def pg_table_meta_data_2(postgres_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2023'
    primary_keys = ['Matrikelnummer']
    data_type_info = {'Matrikelnummer': {'data_type_group': 'integer', 'data_type': 'integer', 'numeric_precision': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'zugelassen': {'data_type_group': 'boolean', 'data_type': 'boolean', 'is_unsigned': True, 'is_nullable': True, 'column_default': 'false', 'is_unique': False, 'auto_increment': False}, 'Vorname': {'data_type_group': 'text', 'data_type': 'character varying', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Nachname': {'data_type_group': 'text', 'data_type': 'character varying', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Note': {'data_type_group': 'text', 'data_type': 'character varying', 'character_max_length': 16, 'is_nullable': True, 'column_default': 'NULL::character varying', 'is_unique': False, 'auto_increment': False}}
    row_count = 11
    return TableMetaData(postgres_engine, table_name, primary_keys, data_type_info, row_count)

@pytest.fixture
def fail_table_meta_data(fail_engine: Engine) -> TableMetaData:
    table_name = 'IDontExist'
    primary_keys = ['KEY']
    data_type_info = {'KEY': {'data_type_group': 'nonsense', 'data_type': 'nonsense'}}
    row_count = 1000000
    return TableMetaData(fail_engine, table_name, primary_keys, data_type_info, row_count)

def test_connect_to_db() -> None:
    maria_engine = connect_to_db(ev.MARIADB_USERNAME, ev.MARIADB_PASSWORD, ev.MARIADB_SERVERNAME, ev.MARIADB_PORTNUMBER, 'MariaTest', 'mariadb', ev.MARIADB_ENCODING)
    postgres_engine = connect_to_db(ev.POSTGRES_USERNAME, ev.POSTGRES_PASSWORD, ev.POSTGRES_SERVERNAME, ev.POSTGRES_PORTNUMBER, 'PostgresTest1', 'postgresql', ev.POSTGRES_ENCODING)
    assert type(maria_engine) == Engine
    assert type(postgres_engine) == Engine
    assert maria_engine.connect().execute(text('SELECT * FROM Vorlesung_Datenbanken_SS2024')) is not None
    assert postgres_engine.connect().execute(text('SELECT * FROM "Vorlesung_Datenbanken_SS2024"')) is not None

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
        


def test_list_all_tables_in_db_with_preview(maria_engine: Engine, postgres_engine: Engine) -> None:
    maria_table_names = ['uebung_datenbanken_ss2024', 'vorlesung_datenbanken_ss2023', 'vorlesung_datenbanken_ss2024']
    postgres_table_names = ['Uebung_Datenbanken_SS2024', 'Vorlesung_Datenbanken_SS2023', 'Vorlesung_Datenbanken_SS2024']
    maria_tables_and_columns, maria_previews, maria_tables_without_pks = list_all_tables_in_db_with_preview(maria_engine)
    postgres_tables_and_columns, postgres_previews, postgres_tables_without_pks = list_all_tables_in_db_with_preview(postgres_engine)
    for table in maria_table_names:
        assert table in maria_tables_and_columns.keys()
    for table in postgres_table_names:
        assert table in postgres_tables_and_columns.keys()
    assert len(maria_previews['uebung_datenbanken_ss2024']) == 20
    assert len(postgres_previews['Uebung_Datenbanken_SS2024']) == 20
    assert len(maria_previews['vorlesung_datenbanken_ss2023']) == 11
    assert len(postgres_previews['Vorlesung_Datenbanken_SS2023']) == 11
    assert len(maria_previews['vorlesung_datenbanken_ss2024']) == 20
    assert len(postgres_previews['Vorlesung_Datenbanken_SS2024']) == 20
    assert len(maria_previews.keys()) == 3
    assert len(postgres_previews.keys()) == 3
    for key in maria_previews.keys():
        for row in maria_previews[key]:
            assert len(row) == len(maria_tables_and_columns[key])
    for key in postgres_previews.keys():
        for row in postgres_previews[key]:
            assert len(row) == len(postgres_tables_and_columns[key])
    assert maria_tables_without_pks == []
    assert postgres_tables_without_pks == []

def test_list_all_tables_in_db_with_preview_exception(fail_engine: Engine) -> None:
    with pytest.raises(DialectError):
        list_all_tables_in_db_with_preview(fail_engine)

def test_get_full_table_ordered_by_primary_key(md_table_meta_data_2: TableMetaData, pg_table_meta_data_2: TableMetaData) -> None:
    converted_maria_result = get_full_table_ordered_by_primary_key(md_table_meta_data_2)
    unconverted_maria_result = get_full_table_ordered_by_primary_key(md_table_meta_data_2, convert = False)
    converted_postgres_result = get_full_table_ordered_by_primary_key(pg_table_meta_data_2)
    unconverted_postgres_result = get_full_table_ordered_by_primary_key(pg_table_meta_data_2, convert = False)
    assert type(converted_maria_result) == list
    assert type(unconverted_maria_result) == CursorResult
    assert type(converted_postgres_result) == list
    assert type(unconverted_postgres_result) == CursorResult
    assert converted_maria_result == converted_postgres_result
    assert converted_maria_result == [[1432209, 'Hendrik', 'Nielsen', 1, '1.0'], [1503456, 'Jessica', 'Wolnitz', 0, None], [2000675, 'Anton', 'Hegl', 0, None], [2111098, 'Zara', 'Lohefalter', 1, '4.0'], [2233449, 'Tatiana', 'Hatt', 0, None], [2340992, 'Carlos', 'Metzger', 1, '2.7'], [2345644, 'Tristan', 'Ingwersen', 1, '5.0'], [2356781, 'Benedikt', 'Friedrichs', 1, 'n.b.'], [2360099, 'Gustav', 'Grant', 1, 'n. b.'], [2398562, 'Karl', 'Heinz', 1, '2.7'], [2400563, 'Gudrun', 'Becker', 0, None]]
    assert converted_postgres_result == [[1432209, 'Hendrik', 'Nielsen', 1, '1.0'], [1503456, 'Jessica', 'Wolnitz', 0, None], [2000675, 'Anton', 'Hegl', 0, None], [2111098, 'Zara', 'Lohefalter', 1, '4.0'], [2233449, 'Tatiana', 'Hatt', 0, None], [2340992, 'Carlos', 'Metzger', 1, '2.7'], [2345644, 'Tristan', 'Ingwersen', 1, '5.0'], [2356781, 'Benedikt', 'Friedrichs', 1, 'n.b.'], [2360099, 'Gustav', 'Grant', 1, 'n. b.'], [2398562, 'Karl', 'Heinz', 1, '2.7'], [2400563, 'Gudrun', 'Becker', 0, None]]
    
def test_get_full_table_ordered_by_primary_key_exception(fail_table_meta_data: TableMetaData) -> None:
    with pytest.raises(DialectError):
        get_full_table_ordered_by_primary_key(fail_table_meta_data)

def test_get_row_count_from_engine(maria_engine: Engine, postgres_engine: Engine) -> None:
    assert get_row_count_from_engine(maria_engine, 'Vorlesung_Datenbanken_SS2024') == 51
    assert get_row_count_from_engine(maria_engine, 'Vorlesung_Datenbanken_SS2023') == 11
    assert get_row_count_from_engine(postgres_engine, 'Vorlesung_Datenbanken_SS2024') == 51
    assert get_row_count_from_engine(postgres_engine, 'Vorlesung_Datenbanken_SS2023') == 11

def test_get_row_count_from_engine_exception(fail_engine: Engine) -> None:
    with pytest.raises(DialectError):
        get_row_count_from_engine(fail_engine, 'inexistent_table')

def test_build_sql_condition() -> None:
    assert build_sql_condition(('matrikelnummer', 'zugelassen'), 'postgresql', 'AND') == 'WHERE matrikelnummer = :matrikelnummer AND zugelassen = :zugelassen'
    assert build_sql_condition(('Matrikelnummer', 'zugelassen'), 'postgresql', 'AND') == 'WHERE "Matrikelnummer" = :Matrikelnummer AND zugelassen = :zugelassen'
    assert build_sql_condition(('matrikelnummer', 'zugelassen'), 'mariadb', 'AND') == 'WHERE matrikelnummer = :matrikelnummer AND zugelassen = :zugelassen'
    assert build_sql_condition(('Matrikelnummer', 'zugelassen'), 'mariadb', 'AND') == 'WHERE Matrikelnummer = :Matrikelnummer AND zugelassen = :zugelassen'
    
def test_build_sql_condition_exception() -> None:    
    with pytest.raises(QueryError):
        build_sql_condition(('Matrikelnummer', 'Punktzahl'), 'postgresql')
        
def test_check_database_encoding(maria_engine: Engine, postgres_engine: Engine) -> None:
    assert check_database_encoding(maria_engine) == 'utf8mb4'
    assert check_database_encoding(postgres_engine) == 'UTF8'

def test_check_database_encoding(fail_engine: Engine) -> None:
    with pytest.raises(DialectError):
        check_database_encoding(fail_engine)

def test_execute_sql_query(md_table_meta_data_1: TableMetaData, pg_table_meta_data_1: TableMetaData) -> None:
    for data in [md_table_meta_data_1, pg_table_meta_data_1]:
        query = text(f'SELECT * FROM {data.table_name} LIMIT 1')
        result = execute_sql_query(md_table_meta_data_1.engine, query)
        assert type(result) is not None
        assert type(result) == CursorResult

def test_execute_sql_query_exceptions(fail_table_meta_data: TableMetaData, postgres_engine) -> None:
    with pytest.raises(Exception):
        execute_sql_query(fail_table_meta_data.engine, text('SELECT *'), None, raise_exceptions = True)
    with pytest.raises(sqlalchemy.exc.ProgrammingError):
        execute_sql_query(postgres_engine, text('SELECT * FROM inexistent_table'), None, raise_exceptions = True)
    
  

def test_get_primary_key_from_engine(maria_engine: Engine, postgres_engine: Engine) -> None:
    assert get_primary_key_from_engine(maria_engine, 'Vorlesung_Datenbanken_SS2024') == ['Matrikelnummer']
    assert get_primary_key_from_engine(postgres_engine, 'Vorlesung_Datenbanken_SS2024') == ['Matrikelnummer']

def test_get_primary_key_from_engine_exceptions(fail_engine: Engine) -> None:
    with pytest.raises(DialectError):
        get_primary_key_from_engine(fail_engine, 'inexistent_table')

def test_check_data_type_meta_data(maria_engine: Engine, postgres_engine: Engine) -> None:
    assert get_data_type_meta_data(maria_engine, 'Vorlesung_Datenbanken_SS2023') == {'Matrikelnummer': {'data_type_group': 'integer', 'data_type': 'int', 'numeric_precision': 10, 'is_nullable': False, 'column_default': None, 'is_unique': True, 'auto_increment': False}, 'Vorname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Nachname': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'zugelassen': {'data_type_group': 'boolean', 'data_type': 'boolean', 'is_nullable': True, 'column_default': '0', 'is_unique': False, 'auto_increment': False}, 'Note': {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 16, 'is_nullable': True, 'column_default': 'NULL', 'is_unique': False, 'auto_increment': False}}
    assert get_data_type_meta_data(postgres_engine, 'Vorlesung_Datenbanken_SS2023') == {'Matrikelnummer': {'data_type_group': 'integer', 'data_type': 'integer', 'numeric_precision': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'zugelassen': {'data_type_group': 'boolean', 'data_type': 'boolean', 'is_unsigned': True, 'is_nullable': True, 'column_default': 'false', 'is_unique': False, 'auto_increment': False}, 'Vorname': {'data_type_group': 'text', 'data_type': 'character varying', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Nachname': {'data_type_group': 'text', 'data_type': 'character varying', 'character_max_length': 32, 'is_nullable': False, 'column_default': None, 'is_unique': False, 'auto_increment': False}, 'Note': {'data_type_group': 'text', 'data_type': 'character varying', 'character_max_length': 16, 'is_nullable': True, 'column_default': 'NULL::character varying', 'is_unique': False, 'auto_increment': False}}

def test_check_data_type_meta_data_exceptions(maria_engine: Engine, fail_engine: Engine) -> None:
    with pytest.raises(DialectError):
        get_data_type_meta_data(fail_engine, 'inexistent_table')
    with pytest.raises(ArgumentError):
        get_data_type_meta_data('Helmut', 'Vorlesung_Datenbanken')
        get_data_type_meta_data(maria_engine, 1111)


### Tests der Funktionen, die keine Exceptions ausgeben ###
def test_convert_string_if_contains_capitals_or_spaces() -> None:
    pg = 'postgresql'
    mdb = 'mariadb'
    string_1 = 'anton'
    string_2 = 'Matrikelnummer'
    string_3 = '"Vorname"'
    string_4 = 'vorige Punktzahl'
    assert convert_string_if_contains_capitals_or_spaces(string_1, mdb) == string_1
    assert convert_string_if_contains_capitals_or_spaces(string_2, mdb) == string_2
    assert convert_string_if_contains_capitals_or_spaces(string_3, mdb) == string_3
    assert convert_string_if_contains_capitals_or_spaces(string_4, mdb) == '"vorige Punktzahl"'
    assert convert_string_if_contains_capitals_or_spaces(string_1, pg) == string_1
    assert convert_string_if_contains_capitals_or_spaces(string_2, pg) == '"Matrikelnummer"'
    assert convert_string_if_contains_capitals_or_spaces(string_3, pg) == string_3
    assert convert_string_if_contains_capitals_or_spaces(string_4, pg) == '"vorige Punktzahl"'

def test_convert_result_to_list_of_lists(maria_engine: Engine, postgres_engine: Engine) -> None:
    maria_result = maria_engine.connect().execute(text('SELECT Matrikelnummer FROM Vorlesung_Datenbanken_SS2023 LIMIT 5'))
    maria_list = convert_result_to_list_of_lists(maria_result)
    postgres_result = postgres_engine.connect().execute(text('SELECT "Matrikelnummer" FROM "Vorlesung_Datenbanken_SS2023" LIMIT 5'))
    postgres_list = convert_result_to_list_of_lists(postgres_result)
    assert type(maria_list) == list
    assert type(postgres_list) == list
    assert len(maria_list) == 5 
    assert len(postgres_list) == 5 
    for row in maria_list:
        assert len(row) == 1
    for row in postgres_list:
        assert len(row) == 1