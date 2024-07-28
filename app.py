from flask import Flask, render_template, request, flash, redirect, session, abort, url_for, jsonify
import os
import re
from waitress import serve
from model.SQLDatabaseError import DatabaseError
from model.loginModel import register_new_user, login_user 
from model.databaseModel import connect_to_db, list_all_tables_in_db, get_column_names_and_datatypes_from_engine, get_primary_key_from_engine, search_string, get_row_count_from_engine, get_full_table, get_full_table_ordered_by_primary_key, get_unique_values_for_attribute, get_row_number_of_affected_entries, update_to_unify_entries

global engine_1
global engine_2
global db_in_use
global tables_in_use
global metadata_table_1
global metadata_table_2

engine_1 = None
engine_2 = None
db_in_use = 0 # 0: keine Engine, 1: engine_1 ist nutzbar, 2: engine_2 ist nutzbar, 3: engine_ und engine_2 sind nutzbar
tables_in_use = 0

app = Flask(__name__, template_folder = 'view/templates', static_folder = 'view/static')



#TODO: Routen '/' und 'login' zusammenführen
@app.route('/')
def start(): 
    if not session.get('logged_in'):
        return render_template('login.html', message = 'Bitte loggen Sie sich ein, um fortzufahren.')
    else:
        return login_to_one_db()
    

@app.route('/login', methods=['POST', 'GET'])
def check_login():
    message = ''
    flash('Hi!')
    if request.method == 'POST':
        result = login_user(request.form['username'], request.form['password'])
        if result[0] == True:
            session['logged_in'] = True
            session['username'] = request.form['username']
            return redirect(url_for('login_to_one_db'))
        else:
            message = result[1]
    return render_template('login.html', message = message)
    
@app.route('/register', methods = ['POST', 'GET'])
def register():
    message = ''
    print(request)
    if request.method == 'POST':
        message = register_new_user(request.form['username'], request.form['password'])
    return render_template('register.html', message = message)

@app.route('/singledb', methods = ['POST', 'GET'])
def login_to_one_db():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    else:
        if request.method == 'POST':
            db_name = request.form['dbname']
            db_dialect = request.form['dbdialect']
            username = request.form['username']
            password = request.form['password']
            host = request.form['hostname']
            port = request.form['portnumber']
            db_encoding = request.form['encoding']
            login_result = login_to_db(username, password, host, port, db_name, db_dialect, db_encoding)
            message = login_result[0]
            status = login_result[1]
            if not status == 0:
                return render_template('singledb.html', username = session['username'], message = message)
            else:
                if 'onedb' in request.form.keys():
                    tables = dict()
                    engine_no = 0
                    if db_in_use == 1:
                        tables = list_all_tables_in_db(engine_1)
                        engine_no = 1
                    elif db_in_use == 2:
                        tables = list_all_tables_in_db(engine_2)
                        engine_no = 2
                    return render_template('tables.html', engine_no = engine_no, db_name = db_name, tables = tables, message = message)
                elif 'twodbs' in request.form.keys():
                    return render_template('tables.html')
        elif request.method == 'GET':
            return render_template('singledb.html', username = session['username'])
        
@app.route('/tables', methods=['POST'])
def select_tables():
    print(request)
    global tables_in_use
    global metadata_table_1
    global metadata_table_2
    engine_no = int(request.form['engineno'])
    engine = None
    table_name = request.form['selectedtable']
    columns = ['Matrikelnummer', 'Vorname', 'Nachname']
    if len(request.form.getlist('selectedtable')) == 1:
        print(tables_in_use)
        print(engine_no)
        if engine_no == 1:
            engine = engine_1
        elif engine_no == 2:
            engine = engine_2
        count = get_row_count_from_engine(engine, table_name)
        if tables_in_use == 0:
            tables_in_use += 1
            metadata_table_1 = TableMetaData(engine, table_name, count)
            columns = metadata_table_1.columns
        elif tables_in_use == 1:
            tables_in_use += 2
            metadata_table_2 = TableMetaData(engine, table_name, count)
            columns = metadata_table_2.columns
        else:
            tables = list_all_tables_in_db(engine)
            return render_template('tables.html', engine_no = engine_no, db_name = engine.url.database, tables = tables, message = 'Sie haben bereits zwei Tabellen ausgewählt.')
        data = convert_result_to_list_of_tuples(get_full_table(engine, table_name))
        return render_template('onetable.html', table_name = table_name, table_columns = columns, data = data)
    else:
        table_names = request.form.getlist('selectedtable')
        print(table_names)
        return render_template('onetable.html', table_name = table_names, table_columns = columns)
   
@app.route('/search', methods = ['POST', 'GET'] )
def search_entries():
    db_name = engine_1.url.database
    table_name = metadata_table_1.table
    table_columns = metadata_table_1.columns
    searched_string = ''
    data = []
    if request.method == 'GET':
        data = convert_result_to_list_of_tuples(get_full_table(engine_1, table_name))
    elif request.method == 'POST':
        column_name = request.form['columntosearch']
        column_names_and_datatypes = dict()
        if column_name == 'all':
            column_names_and_datatypes = metadata_table_1.column_names_and_datatypes
        else:
            column_names_and_datatypes = {column_name: metadata_table_1.column_names_and_datatypes[column_name]}
        string_to_search = request.form['searchstring']
        if string_to_search.strip() == '':
            data = convert_result_to_list_of_tuples(get_full_table(engine_1, table_name))
        else:
            data = convert_result_to_list_of_tuples(search_string(engine_1, table_name, column_names_and_datatypes, string_to_search))
        searched_string = string_to_search
    return render_template('search.html', db_name = db_name, table_name = table_name, searched_string = searched_string, table_columns = table_columns, data = data)

@app.route('/replace')
def search_and_replace_entries():
    db_name = engine_1.url.database
    table_name = request.form['table']
    if request.method == 'GET':
    
        return render_template('replace.html', db_name = db_name, table_name = table_name)
    elif request.method == 'POST':
        return None


@app.route('/unify-selection', methods = ['POST'])
def select_entries_to_unify():
    db_name = engine_1.url.database
    table_name = request.form['tablename']
    column_to_unify = request.form['columntounify']
    data = convert_result_to_list_of_tuples(get_unique_values_for_attribute(engine_1, table_name, column_to_unify))
    return render_template('unify-selection.html', db_name = db_name, table_name = table_name, column_to_unify = column_to_unify, data = data, engine_no = 1)

@app.route('/unify-preview', methods = ['POST'])
def show_affected_entries():
    print(request)
    db_name = engine_1.url.database
    table_name = metadata_table_1.table
    table_columns = metadata_table_1.columns
    column_to_unify = request.form['columntounify']
    new_value = request.form['replacement']
    index_of_affected_attribute = 0
    primary_keys = metadata_table_1.primary_keys
    old_values = list()
    for key in request.form.keys():
        if re.match(r'^[0-9]+$', key):
            old_values.append(request.form[key])
    print(old_values)
    data = convert_result_to_list_of_tuples(get_full_table_ordered_by_primary_key(engine_1, table_name, primary_keys))
    affected_entries = convert_result_to_list_of_tuples(get_row_number_of_affected_entries(engine_1, table_name, column_to_unify, metadata_table_1.primary_keys, old_values))
    affected_rows = []
    for row in affected_entries:
        affected_rows.append(row[0])
    row_total = metadata_table_1.total_row_count
    for index, column in enumerate(table_columns):
        if column == column_to_unify:
            index_of_affected_attribute = index + 1
            break
    
    return render_template('unify-preview.html', db_name = db_name, table_name = table_name, table_columns = table_columns, column_to_unify = column_to_unify, old_values = old_values, new_value = new_value, data = data, index_of_affected_attribute = index_of_affected_attribute, affected_rows = affected_rows, row_total = row_total)

@app.route('/unify', methods = ['GET', 'POST'])
def unify_db_entries():
    db_name = engine_1.url.database
    table_name = metadata_table_1.table
    table_columns = metadata_table_1.columns
    primary_keys = metadata_table_1.primary_keys
    message = ''
    for item in request.args:
        print(item)
    if request.method == 'GET':
        data = convert_result_to_list_of_tuples(get_full_table(engine_1, table_name))
    elif request.method == 'POST':
        attribute_to_change = request.form['columntounify']
        # \ wird beim Auslesen zu \\ -> muss rückgängig gemacht werden, weil Parameter zweimal an HTML-Dokumente gesendet wird
        old_values = request.form['oldvalues'].replace('[', '').replace(']', '').replace('\'', '').replace('\\\\', '\\').split(', ')
        new_value = request.form['newvalue']
        message = 'Änderungen erfolgreich durchgeführt.'
        try:
            update_to_unify_entries(engine_1, table_name, attribute_to_change, old_values, new_value)
        except Exception as error:
            message = str(error)
        data = convert_result_to_list_of_tuples(get_full_table(engine_1, table_name))
    return render_template('unify.html', db_name = db_name, table_columns = table_columns, primary_keys = primary_keys, data = data, table_name = table_name, engine_no = 1, message = message)

@app.route('/disconnect')
def disconnect_from_single_db():
    global engine_1
    global db_in_use
    engine_1 = None
    db_in_use = 0
    return render_template('singledb.html', username = session['username'])


@app.route('/twodbs')
def login_to_two_dbs():
    return None
    
@app.route('/logout')
def logout():
    # Daten der Session entfernen, um den Nutzer auszuloggen
    session.pop('loggedin', None)
    session.pop('username', None)
    # Engines zurücksetzen
    global engine_1
    global engine_2
    global db_in_use
    engine_1 = None
    engine_2 = None
    db_in_use = 0
    # Weiterleitung zur Login-Seite
    return render_template('login.html', 
                           message = 'Sie wurden erfolgreich abgemeldet. \nBitte loggen Sie sich wieder ein, um das Tool zu nutzen.')


    
def login_to_db(username, password, host, port, db_name, db_dialect, db_encoding):
    global engine_1
    global engine_2
    global db_in_use
    message = ''
    status = 0
    if engine_1 and engine_2:
        message = 'Sie haben sich bereits mit zwei Datenbanken verbunden.'
        status = 1
    else:
        try: 
            db_engine = connect_to_db(username, password, host, port, db_name, db_dialect, db_encoding)
        except DatabaseError as error:
            status = 1
            message = str(error)
        else:
            if not engine_1:
                engine_1 = db_engine
                message = f'Verbindung zur Datenbank {db_name} aufgebaut.'
                db_in_use += 1
            elif engine_1 and not engine_2:
                engine_2 = db_engine
                db_in_use += 2
                message = f'Verbindung zur Datenbank {db_name} aufgebaut.'
    return message, status

def convert_result_to_list_of_tuples(sql_result):
    result_list = [tuple(row) for row in sql_result.all()]
    return result_list  

class TableMetaData:
    def __init__(self, engine, table_name, row_count):
        self.engine = engine
        self.table = table_name
        column_names_and_datatypes = get_column_names_and_datatypes_from_engine(engine, table_name)
        primary_keys = get_primary_key_from_engine(engine, table_name)
        self.primary_keys = primary_keys
        self.columns = list(column_names_and_datatypes.keys())
        self.data_types = []
        for key in column_names_and_datatypes.keys():
            self.data_types.append(column_names_and_datatypes[key])
        self.column_names_and_datatypes = column_names_and_datatypes
        self.total_row_count = row_count


if __name__ == '__main__':
    app.secret_key = os.urandom(12)
    serve(app, host = '0.0.0.0', port = 8000)
    