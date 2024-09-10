
from pytest import CaptureFixture
import pytest
from sqlalchemy import Engine, create_engine
from ControllerClasses import TableMetaData
from model.databaseModel import check_data_type_meta_data, get_primary_key_from_engine, get_row_count_from_engine
from model.oneTableModel import escape_string, get_concatenated_string_for_matching, search_string, set_matching_operator_and_cast_data_type
from model.twoTablesModel import list_attributes_to_select
from dotenv import load_dotenv
import os

load_dotenv()
MARIADB_USERNAME = os.getenv('MARIADB_USERNAME')
MARIADB_PASSWORD = os.getenv('MARIADB_PASSWORD')
MARIADB_SERVERNAME = os.getenv('MARIADB_SERVERNAME')
MARIADB_PORTNUMBER = os.getenv('MARIADB_PORTNUMBER')
POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_SERVERNAME = os.getenv('POSTGRES_SERVERNAME')
POSTGRES_PORTNUMBER = os.getenv('POSTGRES_PORTNUMBER')

@pytest.fixture
def db_engine_2() -> Engine:
    return create_engine('mariadb+pymysql://root:arc-en-ciel@localhost:3306/Test?charset=utf8')

@pytest.fixture
def md_table_meta_data_1(db_engine_2: Engine) -> TableMetaData:
    table_name = 'vorlesung_datenbanken_ss2024'
    primary_keys = get_primary_key_from_engine(db_engine_2, table_name)
    data_type_info = check_data_type_meta_data(db_engine_2, table_name)
    row_count = get_row_count_from_engine(db_engine_2, table_name)
    tbl = TableMetaData(db_engine_2, table_name, primary_keys, data_type_info, row_count)
    return tbl

def test_list_attributes_to_select(capsys: CaptureFixture[str]) -> None:
    ls = list_attributes_to_select(['studierende', 'vorname', 'nachname'], 'mariadb')
    with capsys.disabled():
        print(ls)
    assert type(ls) == str
    assert ls == ', '.join(['studierende', 'vorname', 'nachname'])

def test_search_string() -> None:
    engine = create_engine('mariadb+pymysql://root:arc-en-ciel@localhost:3306/Test?charset=utf8')


# def test_get_replacement_information() -> None:

# def test_replace_all_string_occurrences() -> None:

# def test_get_indexes_of_affected_attributes_for_replacing() -> None:

# def test_replace_some_string_occurrences() -> None:

# def test_get_unique_values_for_attribute() -> None:

# def test_update_to_unify_entries() -> None:

# def test_check_data_type_and_constraint_compatibility() -> None:

# def test_get_row_number_of_affected_entries(table_meta_data:TableMetaData, affected_attributes:list[str], old_values:list[str], mode:str, convert:bool = True):

def test_set_matching_operator_and_cast_data_type() -> None:
    assert set_matching_operator_and_cast_data_type('postgresql') == ('ILIKE', 'TEXT')
    assert set_matching_operator_and_cast_data_type('mariadb') == ('LIKE', 'CHAR')

def test_get_concatenated_string_for_matching() -> None:
    assert get_concatenated_string_for_matching('postgresql', 'Datenbanken') == "'%' || :Datenbanken || '%'"
    assert get_concatenated_string_for_matching('mariadb', 'Datenbanken') == "CONCAT('%', CONCAT(:Datenbanken, '%'))"


def test_escape_string() -> None:
    assert escape_string('postgresql', '25%') == '25\%'
    assert escape_string('postgresql', 'uebung_datenbanken') == 'uebung\_datenbanken'
    assert escape_string('postgresql', "O'Brian") == "O\'Brian"
    assert escape_string('postgresql', 'http:\\') == 'http:\\\\'
    assert escape_string('postgresql', '"anfangs"') == '\"anfangs\"'
    assert escape_string('mariadb', '25%') == '25\%'
    assert escape_string('mariadb', 'uebung_datenbanken') == 'uebung\_datenbanken'
    assert escape_string('mariadb', "O'Brian") == "O\'Brian"
    assert escape_string('mariadb', 'http:\\') == 'http:\\\\\\\\'
    assert escape_string('postgresql', '"anfangs"') == '\"anfangs\"'

