from model import loginModel as lModel

# Weitergabe der eingegebenen App-Logindaten an das Model zur Überprüfung, Übermittlung des Ergebnisses zur Darstellung an den View
def login(name, password):
    if lModel.login_user(name, password):
        print(name + " " + password)
        print('Login erfolgreich!')
        return True
    else:
        print('Login fehlgeschlagen.')
        return False
#     error = None
#     if request.method == "POST":
#         if request.form["username"] != "admin" or request.form["password"] != "admin":
#             error = "Invalid credentials. Please try again!"
#         else:
#             return redirect(url_for("home"))

# Weitergabe der eingegebenen Logindaten für die Datenbank an das Model zur Überprüfung, Übermittlung des Ergebnisses zur Darstellung an den View

