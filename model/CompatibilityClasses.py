# Klasse mit statischen Dictionarys für die Kompatibilitätsprüfung zwischen Datentypen in MariaDB und PostgreSQL
# basiert auf der Übersicht von https://stackoverflow.com/questions/1942586/comparison-of-database-column-types-in-mysql-postgresql-and-sqlite-cross-map

# Dictionary für die PostgreSQL-Äquivalente von MariaDB-Datentypen
class MariaToPostgresCompatibility():
    data_types = {
        'tinyint': 'smallint',
        'smallint': 'smallint',
        'smallint unsigned': 'integer',
        'mediumint': 'integer',
        'mediumint unsigned': 'integer',
        'int': 'integer',
        'int unsigned': 'bigint',
        'bigint': 'bigint',
        'bigint unsigned': 'numeric(20)',

        'serial': 'bigserial',

        'tinyint(1)': 'boolean',

        'decimal': 'numeric',
        'double': 'double precision',
        'float': 'real',

        'char': 'character',
        'varchar': 'character varying',
        'tinytext': 'text',
        'text': 'text',
        'mediumtext': 'text',
        'longtext': 'text',

        'date': 'date',
        'datetime': 'timestamp',
        'time': 'time',
        'timestamp': 'timestamp',
    }

# Dictionary für die MariaDB-Äquivalente von PostgreSQL-Datentypen
class PostgresToMariaCompatibility():
    data_types = {
        'smallint': 'smallint',
        'integer': 'int',
        'bigint': 'bigint',

        'smallserial': 'serial',
        'serial': 'serial',
        'bigserial': 'serial',

        'boolean': 'tinyint(1)',

        'double precision': 'double',
        'numeric': 'decimal',
        'real': 'float',

        '"char"': 'char',
        'character': 'char',
        'character varying': 'varchar',

        'date': 'date',
        'time': 'time',
        'timestamp': 'timestamp'
    }