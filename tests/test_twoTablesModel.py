from argparse import ArgumentError
import sys
import pytest
from sqlalchemy import Engine, create_engine, text
from ControllerClasses import TableMetaData
from model.SQLDatabaseError import DialectError
from model.databaseModel import convert_result_to_list_of_lists, convert_string_if_contains_capitals_or_spaces, execute_sql_query, get_data_type_meta_data, get_primary_key_from_engine, get_row_count_from_engine
from model.twoTablesModel import add_constraints_to_new_attribute, build_query_to_add_column, check_arguments_for_joining, check_basic_data_type_compatibility, force_cast_and_match, get_full_column_definition_for_mariadb, join_tables_of_different_dialects_dbs_or_servers, join_tables_of_same_dialect_on_same_server, list_attributes_to_select, simulate_merge_and_build_query
import urllib.parse
# Anpassung der PATH-Variable, damit die Umgebungsvariablen aus environmentVariables.py eingelesen werden können
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
    table_result, column_names, unmatched_rows = join_tables_of_different_dialects_dbs_or_servers(meta_data, attributes_to_join_on, select_1, select_2)
    full_outer_join = join_tables_of_different_dialects_dbs_or_servers(meta_data, attributes_to_join_on, select_1, select_2, full_outer_join = True)[0]
    assert len(full_outer_join) == len(table_result) + unmatched_rows[0] + unmatched_rows[1]
    for line in table_result:
        assert len(line) == len(select_1) + len(select_2)
        assert type(line[0]) == int
        assert type(line[1]) == bool
        assert type(line[2]) == int
    assert table_result == [[0, False, 1869972], [15, False, 1912967], [200, True, 1938205], [25, False, 1972793], [120, False, 2021596], [100, False, 2076750], [54, False, 2120434], [75, False, 2192140], [200, True, 2256812], [210, True, 2261095], [168, False, 2262911], [150, False, 2302766], [210, True, 2320350], [175, True, 2453099], [63, False, 2454294], [97, False, 2507172], [167, True, 2510983], [233, True, 2643692], [75, False, 2695599], [75, False, 2703748], [85, False, 2752103], [0, False, 2814068], [200, True, 2834378], [100, False, 2838526], [132, False, 2885172], [128, False, 2929136], [80, False, 2985690], [65, False, 3078691], [142, False, 3609446], [200, True, 3763593], [175, True, 5181568]]

# Test der Funktion, die die Attributsübertragung simuliert und die Abfrage erstellt, mit der die Operation ausgeführt werden kann
def test_merge(pg_table_meta_data_1: TableMetaData, md_table_meta_data_2: TableMetaData) -> None:
    target_table_name = convert_string_if_contains_capitals_or_spaces(pg_table_meta_data_1.table_name, pg_table_meta_data_1.engine.dialect.name)
    merge_result, merge_query, merge_params = simulate_merge_and_build_query(pg_table_meta_data_1, md_table_meta_data_2, ['Matrikelnummer', 'Matrikelnummer'], 'Punktzahl')
    # Die an erster Stelle ausgegebene Tabelle entspricht der vollen Tabelle. Da bei der Attributsübertragung keine neuen Tupel eingefügt werden,
    # sollte dieses Ergebnis genauso viele Tupel enthalten wie die unbearbeitete Tabelle.
    assert len(convert_result_to_list_of_lists(merge_result)) == pg_table_meta_data_1.total_row_count
    # Sicherstellung, dass die Abfrage die korrekte Form hat
    assert merge_query == 'ALTER TABLE "Vorlesung_Datenbanken_SS2024" ADD COLUMN "Punktzahl" integer; UPDATE "Vorlesung_Datenbanken_SS2024" SET "Punktzahl" = CASE WHEN "Matrikelnummer" = 1869972 THEN :value_1 WHEN "Matrikelnummer" = 1912967 THEN :value_2 WHEN "Matrikelnummer" = 1938205 THEN :value_3 WHEN "Matrikelnummer" = 1972793 THEN :value_4 WHEN "Matrikelnummer" = 2021596 THEN :value_5 WHEN "Matrikelnummer" = 2076750 THEN :value_6 WHEN "Matrikelnummer" = 2120434 THEN :value_7 WHEN "Matrikelnummer" = 2192140 THEN :value_8 WHEN "Matrikelnummer" = 2256812 THEN :value_9 WHEN "Matrikelnummer" = 2261095 THEN :value_10 WHEN "Matrikelnummer" = 2262911 THEN :value_11 WHEN "Matrikelnummer" = 2302766 THEN :value_12 WHEN "Matrikelnummer" = 2320350 THEN :value_13 WHEN "Matrikelnummer" = 2453099 THEN :value_14 WHEN "Matrikelnummer" = 2454294 THEN :value_15 WHEN "Matrikelnummer" = 2507172 THEN :value_16 WHEN "Matrikelnummer" = 2510983 THEN :value_17 WHEN "Matrikelnummer" = 2643692 THEN :value_18 WHEN "Matrikelnummer" = 2695599 THEN :value_19 WHEN "Matrikelnummer" = 2703748 THEN :value_20 WHEN "Matrikelnummer" = 2752103 THEN :value_21 WHEN "Matrikelnummer" = 2814068 THEN :value_22 WHEN "Matrikelnummer" = 2834378 THEN :value_23 WHEN "Matrikelnummer" = 2838526 THEN :value_24 WHEN "Matrikelnummer" = 2885172 THEN :value_25 WHEN "Matrikelnummer" = 2929136 THEN :value_26 WHEN "Matrikelnummer" = 2985690 THEN :value_27 WHEN "Matrikelnummer" = 3078691 THEN :value_28 WHEN "Matrikelnummer" = 3609446 THEN :value_29 WHEN "Matrikelnummer" = 3763593 THEN :value_30 WHEN "Matrikelnummer" = 5181568 THEN :value_31 END;'
    # Sicherstellung, dass alle benötigten Parameter im Parameter-Dictionary enthalten sind
    assert merge_params == {'value_1': 0, 'value_2': 15, 'value_3': 200, 'value_4': 25, 'value_5': 120, 'value_6': 100, 'value_7': 54, 'value_8': 75, 'value_9': 200, 'value_10': 210, 'value_11': 168, 'value_12': 150, 'value_13': 210, 'value_14': 175, 'value_15': 63, 'value_16': 97, 'value_17': 167, 'value_18': 233, 'value_19': 75, 'value_20': 75, 'value_21': 85, 'value_22': 0, 'value_23': 200, 'value_24': 100, 'value_25': 132, 'value_26': 128, 'value_27': 80, 'value_28': 65, 'value_29': 142, 'value_30': 200, 'value_31': 175}
    # Abschließendes Entfernen des neuen Attributs, um die anderen Tests nicht zu beeinflussen
    execute_sql_query(pg_table_meta_data_1.engine, text(f'ALTER TABLE {target_table_name} DROP COLUMN IF EXISTS "Punktzahl"'), commit = True)

# Test der Funktion zum Erzwingen von Typkonversionen bei Joins zwischen verschiedenen Dialekten bzw. Tabellen, die nicht in einer gemeinsamen 
# Abfrage miteinander verbunden werden können
def test_force_cast_and_match() -> None:
    ### übereinstimmende Kombinationen ###
    # Wenn ein Textattribut einen String enthält, der nur aus Ziffern besteht, besteht eine Übereinstimmung mit einem ganzzahligen Attribut
    # sowohl wenn Letzteres in Text konvertiert wird ...
    assert force_cast_and_match('integer', 'text', [23456, '23456'], 1)[0] == True
    # ... als auch wenn der String in eine ganze Zahl konvertiert wird.
    assert force_cast_and_match('integer', 'text', [23456, '23456'], 2)[0] == True
    # wenn eine Dezimalzahl in eine ganze Zahl konvertiert wird, entfallen die Nachkommastellen 
    assert force_cast_and_match('integer', 'decimal', [2400, 2400.5], 2)[0] == True

    ### nicht übereinstimmende Kombinationen ###
    # Wenn ein textbasierter Wert in eine ganze Zahl konvertiert werden soll, tritt ein ValueError auf, d. h. es kann keine Übereinstimmung mit
    # einem ganzzahligen Wert bestehen.
    assert force_cast_and_match('integer', 'text', [23456, 'matrikelnummer'], 2) == False
    # Analog gilt dies für Dezimalzahlen.
    assert force_cast_and_match('decimal', 'text', [0.256, 'matrikelnummer'], 2) == False
    # Wird eine ganze Zahl in eine Dezimalzahl konvertiert, kann sie nur mit einer Dezimalzahl übereinstimmen, deren Nachkommastellen den Wert 0 haben.
    assert force_cast_and_match('integer', 'decimal', [2400, 2400.5], 1) == False
    

# Test, ob bei ungültigen Argumenten in force_cast_and_match ein ArgumentError ausgegeben wird
def test_force_cast_and_match_exceptions() -> None:
    with pytest.raises(ArgumentError):
        # Fehler wegen einer nicht unterstützten Datentypgruppe (ipv4)
        force_cast_and_match('ipv4', 'text', ['255.0.0.0', '255.0.0.0'], 1)
        # Fehler wegen eines falschen Wertes für cast_direction (nicht 1 oder 2)
        force_cast_and_match('integer', 'text', [255, '255'], 3)

def test_build_query_to_add_column(pg_table_meta_data_1: TableMetaData, md_table_meta_data_2: TableMetaData) -> None:
    pg_table_name = convert_string_if_contains_capitals_or_spaces(pg_table_meta_data_1.table_name, pg_table_meta_data_1.engine.dialect.name)
    md_table_name = convert_string_if_contains_capitals_or_spaces(md_table_meta_data_2.table_name, md_table_meta_data_2.engine.dialect.name)
    
    ### Tests für ein ganzzahliges Attribut, das NULL sein darf und nicht einzigartig ist ###
    integer_info = {'data_type_group': 'integer', 'data_type': 'integer', 'numeric_precision': 5, 'is_nullable': True, 'is_unique': False, 'column_default': None}
    # PostgreSQL
    assert build_query_to_add_column(pg_table_meta_data_1, 'Punkte', integer_info) == f'ALTER TABLE {pg_table_name} ADD COLUMN "Punkte" integer;'
    # MariaDB
    assert build_query_to_add_column(md_table_meta_data_2, 'Punkte', integer_info) == f'ALTER TABLE {md_table_name} ADD COLUMN Punkte integer(5);'
    
    ### Tests für ein Textattribut mit einer max. Länge von 32 Zeichen, das NULL sein darf und nicht einzigartig ist ###
    text_info = {'data_type_group': 'text', 'data_type': 'varchar', 'character_max_length': 32, 'is_nullable': True, 'is_unique': False, 'column_default': None}
    # PostgreSQL
    assert build_query_to_add_column(pg_table_meta_data_1, 'Vorname', text_info) == f'ALTER TABLE {pg_table_name} ADD COLUMN "Vorname" varchar(32);'
    # MariaDB
    assert build_query_to_add_column(md_table_meta_data_2, 'Vorname', text_info) == f'ALTER TABLE {md_table_name} ADD COLUMN Vorname varchar(32);'
    
    ### Tests für ein ganzzahliges Attribut mit einzigartigen Werten, das nicht NULL sein darf ###
    unique_integer_info = {'data_type_group': 'integer', 'data_type': 'integer', 'numeric_precision': 11, 'is_nullable': False, 'is_unique': True, 'column_default': None}
    # PostgreSQL
    assert build_query_to_add_column(pg_table_meta_data_1, 'Matrikelnummer', unique_integer_info) == f'ALTER TABLE {pg_table_name} ADD COLUMN "Matrikelnummer" integer UNIQUE;'
    # MariaDB
    assert build_query_to_add_column(md_table_meta_data_2, 'Matrikelnummer', unique_integer_info) == f'ALTER TABLE {md_table_name} ADD COLUMN Matrikelnummer integer(11) UNIQUE;'
    
    ### Tests für ganzzahliges Attribut mit angegebenem Standardwert ###
    default_integer_info = {'data_type_group': 'integer', 'data_type': 'integer', 'numeric_precision': 5, 'is_nullable': False, 'is_unique': False, 'column_default': 0}
    # PostgreSQL
    assert build_query_to_add_column(pg_table_meta_data_1, 'Punktzahl', default_integer_info) == f'ALTER TABLE {pg_table_name} ADD COLUMN "Punktzahl" integer DEFAULT 0;'
    # MariaDB
    assert build_query_to_add_column(md_table_meta_data_2, 'Punktzahl', default_integer_info) == f'ALTER TABLE {md_table_name} ADD COLUMN Punktzahl integer(5) DEFAULT 0;'
    
    ### Test für ein ganzzahliges Attribut ohne Vorzeichen (nur MariaDB) ###
    unsigned_integer_info = {'data_type_group': 'integer', 'data_type': 'int', 'numeric_precision': 11, 'is_nullable': True, 'is_unique': False, 'is_unsigned': True, 'column_default': None}
    assert build_query_to_add_column(md_table_meta_data_2, 'Matrikelnummer', unsigned_integer_info) == f'ALTER TABLE {md_table_name} ADD COLUMN Matrikelnummer int(11) unsigned;'
   
    ### Test für einen Datumsdatentyp mit Standardwert
    date_info = {'data_type_group': 'date', 'data_type': 'date', 'datetime_precision': 6, 'column_default': 'CURRENT_TIMESTAMP'}
    # PostgreSQL
    assert build_query_to_add_column(pg_table_meta_data_1, 'Uhrzeit', date_info) == f'ALTER TABLE {pg_table_name} ADD COLUMN "Uhrzeit" date(6) DEFAULT CURRENT_TIMESTAMP;'
    # MariaDB
    assert build_query_to_add_column(md_table_meta_data_2, 'Uhrzeit', date_info) == f'ALTER TABLE {md_table_name} ADD COLUMN Uhrzeit date(6) DEFAULT CURRENT_TIMESTAMP;'
    

# Kein Test für execute_merge_and_add_constraints, weil darin nur bereits getestete Funktionen aufgerufen werden

def test_add_constraints_to_new_attribute(pg_table_meta_data_1: TableMetaData, md_table_meta_data_2: TableMetaData) -> None:
    target_table_name = convert_string_if_contains_capitals_or_spaces(pg_table_meta_data_1.table_name, pg_table_meta_data_1.engine.dialect.name)
    # Hinzufügen des neuen Attributs ohne Constraints
    execute_sql_query(pg_table_meta_data_1.engine, text(f'ALTER TABLE {target_table_name} ADD COLUMN IF NOT EXISTS "Punktzahl" integer'), commit = True)
    # Übertragen der bestehenden Constraints aus der Zieltabelle
    message = add_constraints_to_new_attribute(pg_table_meta_data_1, md_table_meta_data_2, 'Punktzahl', 'Punktzahl')
    # Da alle Werte des Attributs 'NULL' sind, kann keine NOT-NULL-Constraint hinzugefügt werden. Die bestehende CHECK-Constraint kann jedoch übertragen werden.
    assert message == 'Dem Attribut Punktzahl kann keine NOT-NULL-Constraint hinzugefügt werden, da darin NULL-Werte enthalten sind. Alle 1 CHECK-Constraints konnten erfolgreich hinzugefügt werden.'
    # Abschließendes Entfernen des neuen Attributs, um die anderen Tests nicht zu beeinflussen
    execute_sql_query(pg_table_meta_data_1.engine, text(f'ALTER TABLE {target_table_name} DROP COLUMN IF EXISTS "Punktzahl"'), commit = True)

def test_get_full_column_definition_for_mariadb(md_table_meta_data_1: TableMetaData) -> None:
    column_def = get_full_column_definition_for_mariadb(md_table_meta_data_1, 'Matrikelnummer')
    assert column_def == '"Matrikelnummer" int(11) NOT NULL CHECK ("Matrikelnummer" between 1000000 and 9999999)'


def test_get_full_column_definition_for_mariadb_exceptions(md_table_meta_data_1: TableMetaData, pg_table_meta_data_1: TableMetaData) -> None:
    # Ausgabe eines ArgumentErrors, da die MariaDB-Tabelle Vorlesung_Datenbanken_SS2024 kein Attribut 'zugelassen' aufweist
    with pytest.raises(ArgumentError):
        get_full_column_definition_for_mariadb(md_table_meta_data_1, 'zugelassen')
    # Ausgabe eines DialectErrors, da die Tabelle in pg_table_meta_data_1 zu einer PostgreSQL-Datenbank gehört
    with pytest.raises(DialectError):
        get_full_column_definition_for_mariadb(pg_table_meta_data_1, 'Matrikelnummer')


def test_list_attributes_to_select() -> None:
    ls = list_attributes_to_select(['studierende', 'vorname', 'nachname'], 'mariadb')
    assert type(ls) == str
    assert ls == ', '.join(['studierende', 'vorname', 'nachname'])

def test_check_basic_data_type_compatibility(fail_engine: Engine) -> None:
    meta_data_1 = TableMetaData(fail_engine, 'inexistent_table', ['id'], {'id': {'data_type_group': 'integer', 'data_type': 'integer', 'is_unique': True}, 'comment': {'data_type_group': 'text', 'data_type': 'text', 'is_unique': False}}, 34)
    meta_data_2 = TableMetaData(fail_engine, 'inexistent_table_2', ['id'], {'id': {'data_type_group': 'integer', 'data_type': 'integer', 'is_unique': True}, 'points': {'data_type_group': 'decimal', 'data_type': 'decimal', 'is_unique': False}, 'comment': {'data_type_group': 'text', 'data_type': 'text', 'is_unique': False}}, 11)
    compatibility = check_basic_data_type_compatibility(meta_data_1, meta_data_2)
    # Die beiden Attribute 'id' und die beiden Attribute 'comment' müssten jeweils als vollständig miteinander kompatibel gewertet werden (Code 1)
    assert 1 in compatibility.keys()
    assert compatibility[1] == [('id', 'id'), ('comment', 'comment')]
    # 'id' aus der ersten und 'points' aus der zweiten Tabelle müssten mit dem Code 5 als ggf. uneindeutig mit ggf. nötigen Typkonversionen bewertet werden
    assert 5 in compatibility.keys()
    assert compatibility[5] == [('id', 'points')]
    # 'id' aus der ersten und 'comment' aus der zweiten Tabelle müssten mit dem Code 6 als ggf. uneindeutig mit nötigen Typkonversionen bewertet werden
    # genauso 'comment' aus der ersten und 'id' bzw. 'points' aus der zweiten Tabelle
    assert 6 in compatibility.keys()
    assert compatibility[6] == [('id', 'comment'), ('comment', 'id'), ('comment', 'points')]
     

# Test für die Ausgabe von Fehlermeldungen bei der Überprüfung der Join-Argumente
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