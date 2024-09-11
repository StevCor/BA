import sys
import pytest
from sqlalchemy import Engine, create_engine, text
from ControllerClasses import TableMetaData
from model.databaseModel import get_data_type_meta_data, get_primary_key_from_engine, get_row_count_from_engine
from model.oneTableModel import escape_string, get_concatenated_string_for_matching, get_replacement_information, search_string, set_matching_operator_and_cast_data_type
import urllib.parse
# Anpassung der PATH-Variable, damit die Umgebungsvariablen aus environmentVariables.py eingelesen werden können
sys.path.append('tests')
import environmentVariables as ev


@pytest.fixture
def maria_engine() -> Engine:
    return create_engine(f'mariadb+pymysql://{ev.MARIADB_USERNAME}:{urllib.parse.quote_plus(ev.MARIADB_PASSWORD)}@{ev.MARIADB_SERVERNAME}:{ev.MARIADB_PORTNUMBER}/MariaTest?charset=utf8mb4')

@pytest.fixture
def postgres_engine() -> Engine:
    return create_engine(f'postgresql://{ev.POSTGRES_USERNAME}:{urllib.parse.quote_plus(ev.POSTGRES_PASSWORD)}@{ev.POSTGRES_SERVERNAME}:{ev.POSTGRES_PORTNUMBER}/PostgresTest1', connect_args = {'client_encoding': 'utf8'})

@pytest.fixture
def md_table_meta_data_1(maria_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2024'
    primary_keys = get_primary_key_from_engine(maria_engine, table_name)
    data_type_info = get_data_type_meta_data(maria_engine, table_name)
    row_count = get_row_count_from_engine(maria_engine, table_name)
    return TableMetaData(maria_engine, table_name, primary_keys, data_type_info, row_count)

@pytest.fixture
def md_table_meta_data_2(maria_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2023'
    primary_keys = get_primary_key_from_engine(maria_engine, table_name)
    data_type_info = get_data_type_meta_data(maria_engine, table_name)
    row_count = get_row_count_from_engine(maria_engine, table_name)
    return TableMetaData(maria_engine, table_name, primary_keys, data_type_info, row_count)

@pytest.fixture
def pg_table_meta_data_1(postgres_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2024'
    print(postgres_engine)
    primary_keys = get_primary_key_from_engine(postgres_engine, table_name)
    data_type_info = get_data_type_meta_data(postgres_engine, table_name)
    row_count = get_row_count_from_engine(postgres_engine, table_name)
    return TableMetaData(postgres_engine, table_name, primary_keys, data_type_info, row_count)

@pytest.fixture
def pg_table_meta_data_2(postgres_engine: Engine) -> TableMetaData:
    table_name = 'Vorlesung_Datenbanken_SS2023'
    primary_keys = get_primary_key_from_engine(postgres_engine, table_name)
    data_type_info = get_data_type_meta_data(postgres_engine, table_name)
    row_count = get_row_count_from_engine(postgres_engine, table_name)
    return TableMetaData(postgres_engine, table_name, primary_keys, data_type_info, row_count)


    

def test_search_string(maria_engine:Engine, md_table_meta_data_1:TableMetaData, postgres_engine:Engine, pg_table_meta_data_1:TableMetaData) -> None:
    maria_search_result = search_string(md_table_meta_data_1, 'Jo', ['Vorname', 'Nachname'])
    maria_row_count = maria_engine.connect().execute(text("SELECT COUNT(*) FROM Vorlesung_Datenbanken_SS2024 WHERE Vorname LIKE '%Jo%' OR Nachname LIKE '%Jo%'")).fetchone()[0]
    assert maria_row_count == 9
    assert len(maria_search_result) == maria_row_count

    postgres_search_result = search_string(pg_table_meta_data_1, 'Jo', ['Vorname', 'Nachname'])
    postgres_row_count = postgres_engine.connect().execute(text("SELECT COUNT(*) FROM \"Vorlesung_Datenbanken_SS2024\" WHERE \"Vorname\" ILIKE '%Jo%' OR \"Nachname\" ILIKE '%Jo%'")).fetchone()[0]
    assert postgres_row_count == 9
    assert len(postgres_search_result) == postgres_row_count



def test_get_replacement_information(md_table_meta_data_1, pg_table_meta_data_1) -> None:
    maria_row_nos_and_old_values, maria_occurrence_dict = get_replacement_information(md_table_meta_data_1, [('Matrikelnummer', 0), ('Vorname', 1), ('Nachname', 0)], 'Jo', 'Jojo')
    assert type(maria_row_nos_and_old_values) == dict
    assert type(maria_occurrence_dict) == dict
    assert maria_row_nos_and_old_values == {2: {'old': [1912967, 'Joanna', 'Hayes'], 'positions': [0, 1, 0], 'new': [None, 'Jojoanna', None], 'primary_key': [1912967]}, 19: {'old': [2695599, 'Joel', 'Turner'], 'positions': [0, 1, 0], 'new': [None, 'Jojoel', None], 'primary_key': [2695599]}, 24: {'old': [2838526, 'Joyce', 'Edwards'], 'positions': [0, 1, 0], 'new': [None, 'Jojoyce', None], 'primary_key': [2838526]}, 46: {'old': [4150993, 'Jonathan', 'Fox'], 'positions': [0, 1, 0], 'new': [None, 'Jojonathan', None], 'primary_key': [4150993]}, 48: {'old': [4490484, 'Joseph', 'Robinson'], 'positions': [0, 1, 0], 'new': [None, 'Jojoseph', None], 'primary_key': [4490484]}}
    assert maria_occurrence_dict == {1: {'row_no': 2, 'primary_key': [1912967], 'affected_attribute': 'Vorname'}, 2: {'row_no': 19, 'primary_key': [2695599], 'affected_attribute': 'Vorname'}, 3: {'row_no': 24, 'primary_key': [2838526], 'affected_attribute': 'Vorname'}, 4: {'row_no': 46, 'primary_key': [4150993], 'affected_attribute': 'Vorname'}, 5: {'row_no': 48, 'primary_key': [4490484], 'affected_attribute': 'Vorname'}}

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

