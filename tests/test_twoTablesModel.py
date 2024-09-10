from argparse import ArgumentError
import sys
import pytest
from sqlalchemy import Engine, create_engine, text
from ControllerClasses import TableMetaData
from model.SQLDatabaseError import DialectError
from model.databaseModel import convert_result_to_list_of_lists, convert_string_if_contains_capitals_or_spaces, get_data_type_meta_data, get_primary_key_from_engine, get_row_count_from_engine
from model.twoTablesModel import check_arguments_for_joining, check_basic_data_type_compatibility, get_full_column_definition_for_mariadb, join_tables_of_different_dialects_dbs_or_servers, join_tables_of_same_dialect_on_same_server, list_attributes_to_select, simulate_merge_and_build_query
import urllib.parse
sys.path.append('tests')
import environmentVariables as ev

@pytest.fixture
def db_engine_1() -> Engine:
    return create_engine(f'postgresql://{ev.POSTGRES_USERNAME}:{urllib.parse.quote_plus(ev.POSTGRES_PASSWORD)}@{ev.POSTGRES_SERVERNAME}:{ev.POSTGRES_PORTNUMBER}/PostgresTest1', connect_args = {'client_encoding': 'utf8'})

@pytest.fixture
def db_engine_2() -> Engine:
    return create_engine(f'mariadb+pymysql://{ev.MARIADB_USERNAME}:{urllib.parse.quote_plus(ev.MARIADB_PASSWORD)}@{ev.MARIADB_SERVERNAME}:{ev.MARIADB_PORTNUMBER}/MariaTest?charset=utf8mb4')

@pytest.fixture
def fail_engine() -> Engine:
    return create_engine(f'sqlite:///C:/Benutzer/inexistent_database.db')

@pytest.fixture
def pg_table_meta_data_1(db_engine_1: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2024'
    primary_keys = get_primary_key_from_engine(db_engine_1, table_name)
    data_type_info = get_data_type_meta_data(db_engine_1, table_name)
    row_count = get_row_count_from_engine(db_engine_1, table_name)
    return TableMetaData(db_engine_1, table_name, primary_keys, data_type_info, row_count)

@pytest.fixture
def pg_table_meta_data_2(db_engine_1: Engine) -> TableMetaData:
    table_name = 'Uebung_Datenbanken_SS2024'
    primary_keys = get_primary_key_from_engine(db_engine_1, table_name)
    data_type_info = get_data_type_meta_data(db_engine_1, table_name)
    row_count = get_row_count_from_engine(db_engine_1, table_name)
    return TableMetaData(db_engine_1, table_name, primary_keys, data_type_info, row_count)

@pytest.fixture
def md_table_meta_data_1(db_engine_2: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2024'
    primary_keys = get_primary_key_from_engine(db_engine_2, table_name)
    data_type_info = get_data_type_meta_data(db_engine_2, table_name)
    row_count = get_row_count_from_engine(db_engine_2, table_name)
    return TableMetaData(db_engine_2, table_name, primary_keys, data_type_info, row_count)

@pytest.fixture
def md_table_meta_data_2(db_engine_2: Engine) -> TableMetaData:
    table_name = 'Uebung_Datenbanken_SS2024'
    primary_keys = get_primary_key_from_engine(db_engine_2, table_name)
    data_type_info = get_data_type_meta_data(db_engine_2, table_name)
    row_count = get_row_count_from_engine(db_engine_2, table_name)
    return TableMetaData(db_engine_2, table_name, primary_keys, data_type_info, row_count)

@pytest.fixture
def fail_meta_data(fail_engine: Engine) -> TableMetaData:
    table_name = 'Uebung_Datenbanken_SS2024'
    primary_keys = ['Matrikelnummer']
    data_type_info = {'Matrikelnummer': {'data_type_group': 'integer', 'data_type': 'integer'}}
    row_count = 23
    return TableMetaData(fail_engine, table_name, primary_keys, data_type_info, row_count)



def test_join_tables_of_same_dialect(pg_table_meta_data_1: TableMetaData, pg_table_meta_data_2: TableMetaData) -> None:
    meta_data = [pg_table_meta_data_1, pg_table_meta_data_2]
    attributes_to_join_on = ['Matrikelnummer', 'Matrikelnummer']
    select_1 = ['Matrikelnummer', 'Vorname', 'Nachname']
    select_2 = ['Punktzahl']
    table_result, column_names, unmatched_rows = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2)
    full_outer_join = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2, full_outer_join = True)[0]
    assert type(table_result) == list
    assert type(column_names) == list
    assert type(unmatched_rows) == list
    assert len(full_outer_join) == len(table_result) + unmatched_rows[0] + unmatched_rows[1]
    assert len(table_result[0]) == 4

def test_join_tables_of_same_dialect_with_join_attribute_in_sec_list(pg_table_meta_data_1: TableMetaData, pg_table_meta_data_2: TableMetaData) -> None:
    meta_data = [pg_table_meta_data_1, pg_table_meta_data_2]
    attributes_to_join_on = ['Matrikelnummer', 'Matrikelnummer']
    select_1 = ['Matrikelnummer', 'Vorname', 'Nachname']
    select_2 = ['Matrikelnummer']
    table_result, column_names, unmatched_rows = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2)
    full_outer_join = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2, full_outer_join = True)[0]
    assert len(full_outer_join) == len(table_result) + unmatched_rows[0] + unmatched_rows[1]
    assert len(table_result[0]) == 4

def test_join_tables_of_same_dialect_no_first_attributes(pg_table_meta_data_1: TableMetaData, pg_table_meta_data_2: TableMetaData, capsys: pytest.CaptureFixture[str]) -> None:
    meta_data = [pg_table_meta_data_1, pg_table_meta_data_2]
    attributes_to_join_on = ['Matrikelnummer', 'Matrikelnummer']
    select_1 = []
    select_2 = ['Matrikelnummer', 'Punktzahl']
    table_result, column_names, unmatched_rows = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2)
    full_outer_join = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2, full_outer_join = True)[0]
    assert len(full_outer_join) == len(table_result) + unmatched_rows[0] + unmatched_rows[1]
    assert len(table_result[0]) == 2

def test_join_tables_of_same_dialect_no_second_attributes(pg_table_meta_data_1: TableMetaData, pg_table_meta_data_2: TableMetaData) -> None:
    meta_data = [pg_table_meta_data_1, pg_table_meta_data_2]
    attributes_to_join_on = ['Matrikelnummer', 'Matrikelnummer']
    select_1 = ['Matrikelnummer', 'Vorname', 'Nachname']
    select_2 = []
    table_result, column_names, unmatched_rows = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2)
    full_outer_join = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2, full_outer_join = True)[0]
    assert len(full_outer_join) == len(table_result) + unmatched_rows[0] + unmatched_rows[1]


def test_join_tables_of_different_dialects(pg_table_meta_data_2: TableMetaData, md_table_meta_data_1: TableMetaData) -> None:
    meta_data = [pg_table_meta_data_2, md_table_meta_data_1]
    attributes_to_join_on = ['Matrikelnummer', 'Matrikelnummer']
    select_1 = ['Punktzahl', 'zugelassen']
    select_2 = ['Matrikelnummer']
    res, cols, unmatched = join_tables_of_different_dialects_dbs_or_servers(meta_data, attributes_to_join_on, select_1, select_2)

def test_merge(pg_table_meta_data_1: TableMetaData, md_table_meta_data_2: TableMetaData) -> None:
    target_table_name = convert_string_if_contains_capitals_or_spaces(pg_table_meta_data_1.table_name, pg_table_meta_data_1.engine.dialect.name)
    with pg_table_meta_data_1.engine.connect() as connection:
        connection.execute(text(f'ALTER TABLE {target_table_name} DROP COLUMN IF EXISTS "Punktzahl"'))
        connection.commit()
    merge_result, merge_query, merge_params = simulate_merge_and_build_query(pg_table_meta_data_1, md_table_meta_data_2, ['Matrikelnummer', 'Matrikelnummer'], 'Punktzahl')
    assert len(convert_result_to_list_of_lists(merge_result)) == pg_table_meta_data_1.total_row_count
    assert merge_query == 'ALTER TABLE "Vorlesung_Datenbanken_SS2024" ADD COLUMN "Punktzahl" integer; UPDATE "Vorlesung_Datenbanken_SS2024" SET "Punktzahl" = CASE WHEN "Matrikelnummer" = 1869972 THEN :value_1 WHEN "Matrikelnummer" = 1912967 THEN :value_2 WHEN "Matrikelnummer" = 1938205 THEN :value_3 WHEN "Matrikelnummer" = 1972793 THEN :value_4 WHEN "Matrikelnummer" = 2021596 THEN :value_5 WHEN "Matrikelnummer" = 2076750 THEN :value_6 WHEN "Matrikelnummer" = 2120434 THEN :value_7 WHEN "Matrikelnummer" = 2192140 THEN :value_8 WHEN "Matrikelnummer" = 2256812 THEN :value_9 WHEN "Matrikelnummer" = 2261095 THEN :value_10 WHEN "Matrikelnummer" = 2262911 THEN :value_11 WHEN "Matrikelnummer" = 2302766 THEN :value_12 WHEN "Matrikelnummer" = 2320350 THEN :value_13 WHEN "Matrikelnummer" = 2453099 THEN :value_14 WHEN "Matrikelnummer" = 2454294 THEN :value_15 WHEN "Matrikelnummer" = 2507172 THEN :value_16 WHEN "Matrikelnummer" = 2510983 THEN :value_17 WHEN "Matrikelnummer" = 2643692 THEN :value_18 WHEN "Matrikelnummer" = 2695599 THEN :value_19 WHEN "Matrikelnummer" = 2703748 THEN :value_20 WHEN "Matrikelnummer" = 2752103 THEN :value_21 WHEN "Matrikelnummer" = 2814068 THEN :value_22 WHEN "Matrikelnummer" = 2834378 THEN :value_23 WHEN "Matrikelnummer" = 2838526 THEN :value_24 WHEN "Matrikelnummer" = 2885172 THEN :value_25 WHEN "Matrikelnummer" = 2929136 THEN :value_26 WHEN "Matrikelnummer" = 2985690 THEN :value_27 WHEN "Matrikelnummer" = 3078691 THEN :value_28 WHEN "Matrikelnummer" = 3609446 THEN :value_29 WHEN "Matrikelnummer" = 3763593 THEN :value_30 WHEN "Matrikelnummer" = 5181568 THEN :value_31 END;'
    assert merge_params == {'value_1': 0, 'value_2': 15, 'value_3': 200, 'value_4': 25, 'value_5': 120, 'value_6': 100, 'value_7': 54, 'value_8': 75, 'value_9': 200, 'value_10': 210, 'value_11': 168, 'value_12': 150, 'value_13': 210, 'value_14': 175, 'value_15': 63, 'value_16': 97, 'value_17': 167, 'value_18': 233, 'value_19': 75, 'value_20': 75, 'value_21': 85, 'value_22': 0, 'value_23': 200, 'value_24': 100, 'value_25': 132, 'value_26': 128, 'value_27': 80, 'value_28': 65, 'value_29': 142, 'value_30': 200, 'value_31': 175}
    
#def force_cast_and_match

#def build_query_to_add_column

#def execute_merge_and_add_constraints

#def add_constraints_to_new_attribute

def test_get_full_column_definition_for_mariadb(md_table_meta_data_1) -> None:
    column_def = get_full_column_definition_for_mariadb(md_table_meta_data_1, 'Matrikelnummer')
    assert column_def == '"Matrikelnummer" int(11) NOT NULL CHECK ("Matrikelnummer" between 1000000 and 9999999)'


def test_get_full_column_definition_for_mariadb_exceptions(md_table_meta_data_1, pg_table_meta_data_1) -> None:
    with pytest.raises(ArgumentError):
        get_full_column_definition_for_mariadb(md_table_meta_data_1, 'zugelassen')
    with pytest.raises(DialectError):
        get_full_column_definition_for_mariadb(pg_table_meta_data_1, 'Matrikelnummer')


def test_list_attributes_to_select() -> None:
    ls = list_attributes_to_select(['studierende', 'vorname', 'nachname'], 'mariadb')
    assert type(ls) == str
    assert ls == ', '.join(['studierende', 'vorname', 'nachname'])

#def test_get_data_type_meta_data

def test_check_basic_data_type_compatibility(fail_engine) -> None:
    meta_data_1 = TableMetaData(fail_engine, 'inexistent_table', ['id'], {'id': {'data_type_group': 'integer', 'data_type': 'integer', 'is_unique': True}, 'comment': {'data_type_group': 'text', 'data_type': 'text', 'is_unique': False}}, 34)
    meta_data_2 = TableMetaData(fail_engine, 'inexistent_table_2', ['id'], {'id': {'data_type_group': 'integer', 'data_type': 'integer', 'is_unique': True}, 'points': {'data_type_group': 'decimal', 'data_type': 'decimal', 'is_unique': False}, 'comment': {'data_type_group': 'text', 'data_type': 'text', 'is_unique': False}}, 11)
    compatibility = check_basic_data_type_compatibility(meta_data_1, meta_data_2)
    # Die beiden Attribute 'id' und die beiden Attribute 'comment' müssten mit dem Code 1 als vollständig kompatibel gewertet werden
    assert 1 in compatibility.keys()
    assert compatibility[1] == [('id', 'id'), ('comment', 'comment')]
    # 'id' aus der ersten und 'points' aus der zweiten Tabelle müssten mit dem Code 5 als ggf. uneindeutig mit ggf. nötigen Typkonversionen bewertet werden
    assert 5 in compatibility.keys()
    assert compatibility[5] == [('id', 'points')]
    # 'id' aus der ersten und 'comment' aus der zweiten Tabelle müssten mit dem Code 6 als ggf. uneindeutig mit nötigen Typkonversionen bewertet werden
    # genauso 'comment' aus der ersten und 'id' bzw. 'points' aus der zweiten Tabelle
    assert 6 in compatibility.keys()
    assert compatibility[6] == [('id', 'comment'), ('comment', 'id'), ('comment', 'points')]
     

### Test für die Ausgabe von Fehlermeldungen bei der Überprüfung der Join-Argumente ###
def test_check_arguments_for_joining(pg_table_meta_data_1: TableMetaData, pg_table_meta_data_2: TableMetaData, fail_meta_data: TableMetaData):
    meta_data = [pg_table_meta_data_1, pg_table_meta_data_2]
    # Test, dass bei einem aktuell nicht unterstützten SQL-Dialekt ein DialectError auftritt
    with pytest.raises(DialectError):
        check_arguments_for_joining([pg_table_meta_data_1, fail_meta_data], ['Matrikelnummer', 'Matrikelnummer'], ['Vorname'], ['Punktzahl'])
    # Test für die Ausgabe von ArgumentErrors ...
    with pytest.raises(ArgumentError):
        # ... bei nicht genau zwei angegebenen Join-Attributen
        check_arguments_for_joining(meta_data, ['Matrikelnummer'], ['Vorname'], ['Punktzahl'])
        # ... bei nicht genau zwei TableMetaData-Objekten
        check_arguments_for_joining([pg_table_meta_data_1], ['Matrikelnummer', 'Matrikelnummer'], ['Vorname'], ['Punktzahl'])
        # ... bei leerer Auswahl anzuzeigender Attribute
        check_arguments_for_joining(meta_data, ['Matrikelnummer', 'Matrikelnummer'], [], [])
        # ... und bei falschem Wert für cast_direction.
        check_arguments_for_joining(meta_data, ['Matrikelnummer', 'Matrikelnummer'], ['Vorname'], ['Punktzahl'], cast_direction = 3)
    # Test, dass ein TypeError auftritt, wenn eines der Objekte im ersten Argument nicht vom Typ TableMetaData ist
    with pytest.raises(TypeError):
        check_arguments_for_joining([pg_table_meta_data_1, 0], ['Matrikelnummer', 'Matrikelnummer'], ['Vorname'], ['Punktzahl'])