import os.path
from passlib.hash import pbkdf2_sha256

# Registrierung eines neuen lokalen Nutzers
def register_new_user(username, password):
    #TODO: Überprüfung, ob User schon existiert
    if os.path.exists('user.txt'):
        with open('user.txt', 'r') as f:
            if f.readline().strip() == username:
                print('Benutzer existiert bereits. Bitte melden Sie sich an.')
                return False
    else:
        with open('user.txt', 'w') as f:
            f.write(f'{username}\n{pbkdf2_sha256.hash(password)}')  
            print('Benutzer erfolgreich erstellt.')
            return True  
            

#  Überprüfung der Logindaten für die Web-App, Ausgabe einer Bestätigung oder Fehlermeldung
def login_user(username, password):
    if not os.path.exists('user.txt'):
        print('Anmeldung fehlgeschlagen. Bitte versuchen Sie es erneut.')
        return False
    else:
        with open('user.txt', 'r') as f:
            if not f.readline().strip() == username:
                print('Anmeldung fehlgeschlagen. Bitte versuchen Sie es erneut.')
                return False
            else:
                return pbkdf2_sha256.verify(password, f.readline().strip())

# Überprüfung der Logindaten für eine Datenbank, Ausgabe der aufgebauten Verbindung oder einer Fehlermeldung
if __name__ == '__main__':
    print(login_user('Co', 'nope'))
    print(login_user('Co', 'whoops'))

