from enum import Enum

from model.SQLDatabaseError import DialectError

class MariaInt(Enum):

    BOOL = 0
    TINYINT = 7
    INT1 = 7
    SMALLINT = 15
    INT2 = 15
    MEDIUMINT = 23
    INT3 = 23
    INT = 31
    INT4 = 31
    INTEGER = 31
    BIGINT = 63
    INT8 = 63
    #https://stackoverflow.com/questions/41407414/convert-string-to-enum-in-python
    @classmethod
    def value_of(cls, value:str):
        for k, v in cls.__members__.items():
            if k == value.upper():
                return v
        else:
            raise ValueError(f"'Kein Eintrag in der Enum {cls.__name__}' für den Wert '{value}' gefunden.")

class PostgresInt(Enum):

    SMALLINT = 7
    INTEGER = 31
    BIGINT = 63

    @classmethod
    def value_of(cls, value:str):
        for k, v in cls.__members__.items():
            if k == value.upper():
                return v
        else:
            raise ValueError(f"'Kein Eintrag in der Enum {cls.__name__}' für den Wert '{value}' gefunden.")


def get_int_value_by_dialect_name(dialect_name:str, dtype:str):
    if dialect_name == 'mariadb':
        return MariaInt.value_of(dtype)
    elif dialect_name == 'postgresql':
        return PostgresInt.value_of(dtype)
    else:
        raise DialectError(f'Der SQL-Dialekt {dialect_name} wird nicht unterstützt.')