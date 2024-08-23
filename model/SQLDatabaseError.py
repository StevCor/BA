class DatabaseError(Exception):
    """Exception für falsche Login-Eingaben für die Datenbank."""
    def __init__(self, message:str):
        super().__init__(f'Fehler beim Aufbau der Datenbankverbindung. {message}')

class QueryError(Exception):
    """Exception für Fehler bei der Erstellung von Datenbankanfragen."""
    def __init__(self, message:str):
        super().__init__(f'Fehler beim Erstellen der Anfrage. {message}')

class UpdateError(Exception):
    """Exception für Fehler bei Änderungen der Datenbank."""
    def __init__(self, message:str):
        super().__init__(f'Fehler beim Ausführen der Änderung. {message}')

class DialectError(Exception):
    """Exception für Fehler durch Eingabe eines nicht implementierten SQL-Dialekts."""
    def __init__(self, message:str):
        super().__init__(f'SQL-Dialekt-Fehler. {message}')
