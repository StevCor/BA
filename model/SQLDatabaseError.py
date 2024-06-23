class DatabaseError(Exception):
    """Exception für falsche Login-Eingaben für die Datenbank."""

    def __init__(self, message):
        super().__init__(f'Fehler beim Aufbau der Datenbankverbindung. {message}')
