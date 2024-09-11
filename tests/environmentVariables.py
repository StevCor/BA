# Klasse zur Vermeidung von Boilerplate-Code zur Einbindung der Umgebungsvariablen in den Tests
import os
from dotenv import load_dotenv

# Beziehen der Umgebungsvariablen für den Datenbankzugriff in den Tests
load_dotenv()
# Datenbankbenutzername, Datenbankpasswort, Servername, Portnummer und Zeichencodierung für MariaDB
MARIADB_USERNAME = os.getenv('MARIADB_USERNAME')
MARIADB_PASSWORD = os.getenv('MARIADB_PASSWORD')
MARIADB_SERVERNAME = os.getenv('MARIADB_SERVERNAME')
MARIADB_PORTNUMBER = os.getenv('MARIADB_PORTNUMBER')
MARIADB_ENCODING = os.getenv('MARIADB_ENCODING')
# Datenbankbenutzername, Datenbankpasswort, Servername, Portnummer und Zeichencodierung für PostgreSQL
POSTGRES_USERNAME = os.getenv('POSTGRES_USERNAME')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_SERVERNAME = os.getenv('POSTGRES_SERVERNAME')
POSTGRES_PORTNUMBER = os.getenv('POSTGRES_PORTNUMBER')
POSTGRES_ENCODING = os.getenv('POSTGRES_ENCODING')

# Anlegen eines statischen Sets, das in den Testdateien verwendet werden kann
environment_variables = {
    MARIADB_USERNAME,
    MARIADB_PASSWORD,
    MARIADB_SERVERNAME,
    MARIADB_PORTNUMBER,
    MARIADB_ENCODING,
    POSTGRES_USERNAME,
    POSTGRES_PASSWORD,
    POSTGRES_SERVERNAME,
    POSTGRES_PORTNUMBER,
    POSTGRES_ENCODING
}
