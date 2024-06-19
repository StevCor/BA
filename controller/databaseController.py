from model import databaseModel as dModel
# Weitergabe der Parameter für den Aufbau der Verbindung zu einer Datenbank an das Model

def connect_to_db(username, password, host, port, db_name, db_dialect, db_encoding):
    return dModel.build_engine_to_connect_to_db(username, password, host, port, db_name, db_dialect)


# Weitergabe der Parameter für eine Datenbankabfrage an das Model, Ergebnisübermittlung an den View


# bei Änderungen: Auslösen der Datenbankabfrage für die Änderung über das Model,
# Übermittlung des Ergebnisses zur Anzeige als Vorschau an den View, Auslösen der Anzeige
# einer Bestätigungsabfrage über den View, Auslösen der Änderung über das Model bei
# Bestätigung, Auslösen einer Benachrichtigung über die (ggf. nicht vorgenommene)
# Änderung über den View