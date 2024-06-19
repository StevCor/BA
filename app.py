from flask import Flask, render_template, request, flash, redirect, session, abort, url_for
from sqlalchemy import text
import os
from waitress import serve
from controller.loginController import login, register_user
from controller.databaseController import connect_to_db

global engine_1
global engine_2
global db_in_use

engine_1 = None
engine_2 = None
db_in_use = 0


app = Flask(__name__, template_folder = 'view/templates', static_folder = 'view/static')



#TODO: Routen '/' und 'login' zusammenführen
@app.route('/')
def start(): 
    if not session.get('logged_in'):
        return render_template('login.html', message = 'Bitte loggen Sie sich ein, um fortzufahren.')
    else:
        return index()
    

@app.route('/login', methods=['POST', 'GET'])
def check_login():
    message = ''
    flash('Hi!')
    if request.method == 'POST':
        result = login(request.form['username'], request.form['password'])
        if result[0] == True:
            session['logged_in'] = True
            session['username'] = request.form['username']
            return redirect(url_for('index'))
        else:
            message = result[1]
    return render_template('login.html', message = message)
    
@app.route('/register', methods=['POST', 'GET'])
def register():
    message = ''
    print(request)
    if request.method == 'POST':
        message = register_user(request.form['username'], request.form['password'])
    return render_template('register.html', message = message)

@app.route('/index')
def index():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    else:
        return render_template('index.html', username = session['username'])
    
def login_to_db(username, password, host, port, db_name, db_dialect, db_encoding):
    global engine_1
    global engine_2
    global db_in_use
    if engine_1 and engine_2:
        print('Sie haben sich bereits mit zwei Datenbanken verbunden.')
    elif not engine_1:
        engine_1 = connect_to_db(username, password, host, port, db_name, db_dialect, db_encoding)
        print(f'Verbindung zur Datenbank {db_name} aufgebaut.')
        if engine_2:
            db_in_use = 2
        else:
            db_in_use = 1
    elif engine_1 and not engine_2:
        engine_2 = connect_to_db(username, password, host, port, db_name, db_dialect, db_encoding)
        db_in_use = 2
        print(f'Verbindung zur Datenbank {db_name} aufgebaut.')


def logout():
    # Daten der Session entfernen, um den Nutzer auszuloggen
    session.pop('loggedin', None)
    session.pop('username', None)
    # Weiterleitung zur Login-Seite
    return render_template('login.html', 
                           message = 'Sie wurden erfolgreich abgemeldet. \nBitte loggen Sie sich wieder ein, um das Tool zu nutzen.')


if __name__ == '__main__':
    login_to_db('postgres', 'arc-en-ciel', 'localhost', 5432, 'Test', 'PostgreSQL', 'utf8')
    with engine_1.connect() as conn:
        result = conn.execute(text('SELECT * FROM studierende'))
        for row in result.all():
            print(row)

    app.secret_key = os.urandom(12)
    serve(app, host = '0.0.0.0', port = 8000)
    