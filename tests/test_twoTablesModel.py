import pytest
from sqlalchemy import Engine, create_engine, text
from ControllerClasses import TableMetaData
from model.databaseModel import check_data_type_meta_data, get_primary_key_from_engine, get_row_count_from_engine
from model.twoTablesModel import join_tables_of_different_dialects_dbs_or_servers, join_tables_of_same_dialect_on_same_server, simulate_merge_and_build_query

@pytest.fixture
def db_engine_1() -> Engine:
    return create_engine('postgresql://postgres:arc-en-ciel@localhost:5432/Test', connect_args = {'client_encoding': 'utf8'})

@pytest.fixture
def db_engine_2() -> Engine:
    return create_engine('mariadb+pymysql://root:arc-en-ciel@localhost:3306/Test?charset=utf8')

@pytest.fixture
def fail_engine_1() -> Engine:
    return create_engine('mariadb+pymysql://root:arc-en-ciel@localhost:3307/Test?charset=utf8')

@pytest.fixture
def fail_engine_2() -> Engine:
    return create_engine('postgresql://postgres:arc-en-ciel@www.google.com:3306/Test?charset=utf8')

@pytest.fixture
def pg_table_meta_data_1(db_engine_1: Engine) -> TableMetaData:
    table_name = 'studierende'
    primary_keys = get_primary_key_from_engine(db_engine_1, table_name)
    data_type_info = check_data_type_meta_data(db_engine_1, table_name)
    row_count = get_row_count_from_engine(db_engine_1, table_name)
    tbl = TableMetaData(db_engine_1, table_name, primary_keys, data_type_info, row_count)
    return tbl

@pytest.fixture
def pg_table_meta_data_2(db_engine_1: Engine) -> TableMetaData:
    table_name = 'punkte'
    primary_keys = get_primary_key_from_engine(db_engine_1, table_name)
    data_type_info = check_data_type_meta_data(db_engine_1, table_name)
    row_count = get_row_count_from_engine(db_engine_1, table_name)
    tbl = TableMetaData(db_engine_1, table_name, primary_keys, data_type_info, row_count)
    return tbl

@pytest.fixture
def md_table_meta_data_1(db_engine_2: Engine) -> TableMetaData:
    table_name = 'uebung_datenbanken'
    primary_keys = get_primary_key_from_engine(db_engine_2, table_name)
    data_type_info = check_data_type_meta_data(db_engine_2, table_name)
    row_count = get_row_count_from_engine(db_engine_2, table_name)
    tbl = TableMetaData(db_engine_2, table_name, primary_keys, data_type_info, row_count)
    return tbl

def test_join_tables_of_same_dialect(pg_table_meta_data_1: TableMetaData, pg_table_meta_data_2: TableMetaData) -> None:
    meta_data = [pg_table_meta_data_1, pg_table_meta_data_2]
    attributes_to_join_on = ['matrikelnummer', 'matrikelnummer']
    select_1 = ['matrikelnummer', 'vorname', 'nachname']
    select_2 = ['punktzahl']
    table_result, column_names, unmatched_rows = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2)
    full_outer_join = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2, True)[0]
    assert type(table_result) == list
    assert type(column_names) == list
    assert type(unmatched_rows) == list
    assert len(full_outer_join) == len(table_result) + unmatched_rows[0] + unmatched_rows[1]
    assert len(table_result[0]) == 4

def test_join_tables_of_same_dialect_with_join_attribute_in_sec_list(pg_table_meta_data_1: TableMetaData, pg_table_meta_data_2: TableMetaData) -> None:
    meta_data = [pg_table_meta_data_1, pg_table_meta_data_2]
    attributes_to_join_on = ['matrikelnummer', 'matrikelnummer']
    select_1 = ['matrikelnummer', 'vorname', 'nachname']
    select_2 = ['matrikelnummer']
    table_result, column_names, unmatched_rows = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2)
    full_outer_join = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2, full_outer_join = True)[0]
    assert len(full_outer_join) == len(table_result) + unmatched_rows[pg_table_meta_data_1.table_name] + unmatched_rows[pg_table_meta_data_2.table_name]
    assert len(table_result[0]) == 3

def test_join_tables_of_same_dialect_no_first_attributes(pg_table_meta_data_1: TableMetaData, pg_table_meta_data_2: TableMetaData, capsys: pytest.CaptureFixture[str]) -> None:
    meta_data = [pg_table_meta_data_1, pg_table_meta_data_2]
    attributes_to_join_on = ['matrikelnummer', 'matrikelnummer']
    select_1 = []
    select_2 = ['matrikelnummer', 'punktzahl']
    table_result, column_names, unmatched_rows = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2)
    full_outer_join = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2, True)[0]
    assert len(full_outer_join) == len(table_result) + unmatched_rows[pg_table_meta_data_1.table_name] + unmatched_rows[pg_table_meta_data_2.table_name]
    assert len(table_result[0]) == 2

def test_join_tables_of_same_dialect_no_second_attributes(pg_table_meta_data_1: TableMetaData, pg_table_meta_data_2: TableMetaData, capsys: pytest.CaptureFixture[str]) -> None:
    meta_data = [pg_table_meta_data_1, pg_table_meta_data_2]
    attributes_to_join_on = ['matrikelnummer', 'matrikelnummer']
    select_1 = ['matrikelnummer', 'vorname', 'nachname']
    select_2 = []
    table_result, column_names, unmatched_rows = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2)
    full_outer_join = join_tables_of_same_dialect_on_same_server(meta_data, attributes_to_join_on, select_1, select_2, True)[0]
    assert len(full_outer_join) == len(table_result) + unmatched_rows[pg_table_meta_data_1.table_name] + unmatched_rows[pg_table_meta_data_2.table_name]
    with capsys.disabled():
        print(len(full_outer_join))
        print(len(table_result))
        print(unmatched_rows[pg_table_meta_data_1.table_name])
        print(unmatched_rows[pg_table_meta_data_2.table_name])
        for row in table_result:
            print(row)

def test_join_tables_of_different_dialects(pg_table_meta_data_2: TableMetaData, md_table_meta_data_1: TableMetaData, capsys: pytest.CaptureFixture[str]) -> None:
    meta_data = [pg_table_meta_data_2, md_table_meta_data_1]
    attributes_to_join_on = ['punktzahl', 'zugelassen']
    select_1 = ['matrikelnummer', 'punktzahl']
    select_2 = ['punktzahl', 'zugelassen']
    res, cols, unmatched = join_tables_of_different_dialects_dbs_or_servers(meta_data, attributes_to_join_on, select_1, select_2)
    with capsys.disabled():
        for row in res:
            print(row)
        print('Spaltennamen: ', cols)
    
# weitere Tests:
# ArgumentError bei ungleichen Servernamen
# ArgumentError bei ungleichen Portnummern
# ArgumentError bei ungleichen Dialekten
# ArgumentError bei Anzahl der Join-Attribute != 2
# ArgumentError bei Anzahl der TableMetaData != 2
# ArgumentError bei Anzahl auszuwählender Attribute == 0
# ArgumentError bei cast_direction not in (None, 1, 2)
# DialectError bei dialect != postgres oder mariadb
# TypeError, wenn keine TableMetaData übergeben werden

def test_merge(pg_table_meta_data_2: TableMetaData, md_table_meta_data_1: TableMetaData, capsys: pytest.CaptureFixture[str]) -> None:
    with pg_table_meta_data_2.engine.connect() as connection:
        connection.execute(text('ALTER TABLE punkte DROP COLUMN IF EXISTS "Punktzahl"'))
        connection.commit()
    with capsys.disabled():
        simulate_merge_and_build_query(pg_table_meta_data_2, md_table_meta_data_1, ['matrikelnummer', 'Matrikelnummer'], 'Punktzahl')