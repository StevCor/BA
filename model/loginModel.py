import os.path
import re
from passlib.hash import pbkdf2_sha256

def register_new_user(user_name, password):
    """Regstriert einen neuen lokalen Nutzer
    
    user_name: Benutzername als String
    
    password: Passwort als String.
    
    Ausgabe einer Fehler- oder Erfolgsmeldung zur Anzeige in der App."""

    message = ''
    # Öffnen der Benutzerdatendatei mit Schreibrechten
    with open('user.txt', 'a+', encoding = 'utf-8') as f:
        # Datei-Offset auf den Anfang der Datei setzen, damit das Einlesen mit dem Modus a+ (append and update) funktioniert
        f.seek(0)
        # Einlesen der nächsten Zeile
        line = f.readline()
        # Alle Zeilen nach dem eingegebenen Benutzernamen durchsuchen
        while line:
            # Wird dieser gefunden, ist eine Registrierung unter diesem Namen nicht möglich, aber ein Login.
            if line.strip() == user_name:
                return 'Benutzer existiert bereits. Bitte melden Sie sich an.'
            else:
                # Zeile mit Passwort überspringen
                f.readline()
                # nächsten Benutzernamen einlesen
                line = f.readline()
        # Überprüfung, dass der Benutzername nur alphanumerische Zeichen enthält
        if not re.match(r'[A-Za-z0-9]+', user_name):
            message = 'Der Benutzername darf nur Buchstaben und Zahlen enthalten.'
        # Überprüfung, dass ein Benutzername und ein Passwort angegeben sind
        elif not user_name or not password:
            message = 'Bitte füllen Sie alle Felder aus!'
        # Speichern der neuen Anmeldedaten: Benutzername in einer Zeile, dann das SHA-256-verschlüsselte Passwort in der nächsten
        else:
            f.write(f'{user_name}\n{pbkdf2_sha256.hash(password)}\n')  
            message = 'Benutzer erfolgreich erstellt. Sie können sich nun anmelden.'
    return message
    
            

def login_user(user_name, password):
    """Überprüfung der Logindaten für die Web-App
    
    Ausgabe einer Bestätigung oder Fehlermeldung"""

    message = ''
    # Wenn die Datei user.txt noch nicht existiert, wurde noch kein Nutzer registriert, sodass auch kein Login erfolgen kann
    if not os.path.exists('user.txt'):
        message = 'Anmeldung fehlgeschlagen. Bitte versuchen Sie es erneut.'
        return False, message
    else:
        # Anderenfalls wird überprüft, ob der Benutzername in der Datei enthalten ist
        with open('user.txt', 'r', encoding = 'utf-8') as f:
            if not f.readline().strip() == user_name:
                # Falls nicht, ist keine Anmeldung möglich.
                message = 'Anmeldung fehlgeschlagen. Bitte versuchen Sie es erneut.'
                return False, message
            # Ist der Benutzername vorhanden, wird überprüft, ob das Passwort mit dem entschlüsselten gespeicherten Passwort übereinstimmt.
            else:
                return pbkdf2_sha256.verify(password, f.readline().strip()), message

