from model import loginModel as lModel

# Weitergabe der eingegebenen App-Logindaten an das Model zur Überprüfung, Übermittlung des Ergebnisses zur Darstellung an den View
def login(name, password):
    result = lModel.login_user(name, password)
    if result[0]:
        print(name + " " + password)
        print('Login erfolgreich!')
    else:
        print('Login fehlgeschlagen.')
    
    print(result)
    return result
#     error = None
#     if request.method == "POST":
#         if request.form["username"] != "admin" or request.form["password"] != "admin":
#             error = "Invalid credentials. Please try again!"
#         else:
#             return redirect(url_for("home"))

# Weitergabe der eingegebenen Logindaten für die Datenbank an das Model zur Überprüfung, Übermittlung des Ergebnisses zur Darstellung an den View

# Registrierung eines neuen Nutzers
def register_user(name, password):
    return lModel.register_new_user(name, password)