import os.path
import re
from passlib.hash import pbkdf2_sha256

# Registrierung eines neuen lokalen Nutzers
def register_new_user(username, password):
    message = ''
    with open('user.txt', 'a+', encoding = 'utf-8') as f:
        # Datei-Offset auf den Anfang der Datei setzen, damit das Einlesen mit dem Modus a+ (append and update) funktioniert
        f.seek(0)
        line = f.readline()
        while line:
            if line.strip() == username:
                return 'Benutzer existiert bereits. Bitte melden Sie sich an.'
            else:
                # Zeile mit Passwort überspringen
                f.readline()
                # nächsten Benutzernamen einlesen
                line = f.readline()
        if not re.match(r'[A-Za-z0-9]+', username):
            message = 'Der Benutzername darf nur Buchstaben und Zahlen enthalten.'
        elif not username or not password:
            message = 'Bitte füllen Sie alle Felder aus!'
        else:
            f.write(f'{username}\n{pbkdf2_sha256.hash(password)}\n')  
            message = 'Benutzer erfolgreich erstellt. Sie können sich nun anmelden.'
    return message
    
            

#  Überprüfung der Logindaten für die Web-App, Ausgabe einer Bestätigung oder Fehlermeldung
def login_user(username, password):
    message = None
    if not os.path.exists('user.txt'):
        message = 'Anmeldung fehlgeschlagen. Bitte versuchen Sie es erneut.'
        return False, message
    else:
        with open('user.txt', 'r', encoding = 'utf-8') as f:
            if not f.readline().strip() == username:
                message = 'Anmeldung fehlgeschlagen. Bitte versuchen Sie es erneut.'
                return False, message
            else:
                return pbkdf2_sha256.verify(password, f.readline().strip()), message

# Überprüfung der Logindaten für eine Datenbank, Ausgabe der aufgebauten Verbindung oder einer Fehlermeldung
if __name__ == '__main__':
    print(register_new_user('Co', 'arc-en-ciel'))

