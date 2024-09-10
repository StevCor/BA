import pytest
from model.SQLDatabaseError import QueryError
from model.databaseModel import build_sql_condition, convert_string_if_contains_capitals_or_spaces

# def test_connect_to_db() -> None:

# def test_list_all_tables_in_db_with_preview() -> None:

# def test_get_full_table_ordered_by_primary_key() -> None:

# def test_get_row_count_from_engine() -> None:

def test_build_sql_condition() -> None:
    assert build_sql_condition(('matrikelnummer', 'zugelassen'), 'postgresql', 'AND') == 'WHERE matrikelnummer = :matrikelnummer AND zugelassen = :zugelassen'
    assert build_sql_condition(('Matrikelnummer', 'zugelassen'), 'postgresql', 'AND') == 'WHERE "Matrikelnummer" = :Matrikelnummer AND zugelassen = :zugelassen'
    assert build_sql_condition(('matrikelnummer', 'zugelassen'), 'mariadb', 'AND') == 'WHERE matrikelnummer = :matrikelnummer AND zugelassen = :zugelassen'
    assert build_sql_condition(('Matrikelnummer', 'zugelassen'), 'mariadb', 'AND') == 'WHERE Matrikelnummer = :Matrikelnummer AND zugelassen = :zugelassen'
    with pytest.raises(QueryError):
        build_sql_condition(('Matrikelnummer', 'Punktzahl'), 'postgresql')
        
# def test_check_database_encoding() -> None:

# def test_execute_sql_query() -> None:

# def test_get_primary_key_from_engine() -> None:

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

##def test_convert_result_to_list_of_lists() -> None:

#def test_check_data_type_meta_data() -> None:

