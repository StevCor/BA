import datetime
from flask import Flask, render_template, request, flash, redirect, session, url_for
import os
import re
from waitress import serve
from ControllerClasses import TableMetaData
from controllerFunctions import check_validity_of_input_and_searched_value
from model.SQLDatabaseError import DatabaseError, DialectError
from model.loginModel import register_new_user, login_user 
from model.databaseModel import check_data_type_meta_data, connect_to_db, get_column_names_data_types_and_max_length, get_primary_key_from_engine, get_row_count_from_engine, list_all_tables_in_db_with_preview, get_full_table_ordered_by_primary_key
from model.oneTableModel import get_replacement_information, get_row_number_of_affected_entries, get_unique_values_for_attribute, replace_all_string_occurrences, replace_some_string_occurrences, search_string, update_to_unify_entries
from model.twoTablesModel import check_basic_data_type_compatibility, join_tables_of_different_dialects_dbs_or_servers, join_tables_of_same_dialect_on_same_server

global engine_1
global engine_2
global db_in_use
global tables_in_use
global meta_data_table_1
global meta_data_table_2
global replacement_occurrence_dict
global replacement_data_dict
global basic_compatibility_list
global data_type_information

engine_1 = None
engine_2 = None
db_in_use = 0 # 0: keine Engine, 1: engine_1 ist nutzbar, 2: engine_2 ist nutzbar, 3: engine_ und engine_2 sind nutzbar
tables_in_use = 0
basic_compatibility_list = None
data_type_information = None

app = Flask(__name__, template_folder = 'view/templates', static_folder = 'view/static')



@app.route('/')
def start(): 
    return redirect(url_for('check_login'))
    

@app.route('/login', methods=['POST', 'GET'])
def check_login():
    if session.get('logged_in'):
        return redirect(url_for('show_db_login_page', engine_no = 1))
    message = 'Bitte loggen Sie sich ein, um das Tool zu nutzen.'
    if request.method == 'POST':
        result = login_user(request.form['username'], request.form['password'])
        if result[0] == True:
            session['logged_in'] = True
            session['username'] = request.form['username']
            return redirect(url_for('show_db_login_page', engine_no = 1))
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

@app.route('/connect-to-db<int:engine_no>', methods = ['GET', 'POST'])
def show_db_login_page(engine_no):
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    else:
        if request.method == 'GET':
            if engine_no == 2:
                if engine_1.dialect.name == 'mariadb':
                    sql_dialect = 'MariaDB'
                elif engine_1.dialect.name == 'postgresql':
                    sql_dialect = 'PostgreSQL'
                else:
                    raise DialectError(f'Der SQL-Dialekt {engine_1.dialect.name} wird nicht unterstützt.')
                message = f'Tabelle {meta_data_table_1.table_name} der Datenbank {engine_1.url.database} ({sql_dialect}) ausgewählt.'
            else:
                message = ''
            return render_template('db-connect.html', username = session['username'], engine_no = engine_no, engine_1 = engine_1, message = message)
        
@app.route('/connect-to-db', methods = ['GET', 'POST'])
def set_up_db_connection():
    if not session.get('logged_in') or request.method == 'GET':
        return redirect(url_for('start'))
    elif request.method == 'POST':
        if 'db-one' in request.form.keys():
            db_name = request.form['db-name1']
            db_dialect = request.form['db-dialect1']
            username = request.form['user-name1']
            password = request.form['password1']
            host = request.form['host-name1']
            port = request.form['port-number1']
            db_encoding = request.form['encoding1']
        elif 'db-two' in request.form.keys():
            db_name = request.form['db-name2']
            db_dialect = request.form['db-dialect2']
            username = request.form['user-name2']
            password = request.form['password2']
            host = request.form['host-name2']
            port = request.form['port-number2']
            db_encoding = request.form['encoding2']
        engine_no = int(request.form['engine-no'])
        login_result = login_to_db(username, password, host, port, db_name, db_dialect, db_encoding)
        if not login_result == 0:
            message = ''
            if login_result == 1:
                message = 'Sie haben sich bereits mit zwei Datenbanken verbunden.'
            elif login_result == 2:
                message = 'Beim Aufbau der Datenbankverbindung ist ein Fehler aufgetreten. Bitte versuchen Sie es erneut.'
            return render_template('db-connect.html', username = session['username'], engine_no = engine_no, message = message)
        else:
            global engine_2
            if 'db-one' in request.form.keys():
                engine_no = 1
            elif 'db-two' in request.form.keys() and engine_1.url == engine_2.url:
                engine_2 = None
                message = 'Bitte verbinden Sie sich mit einer anderen Datenbank.'
                return render_template('db-connect.html', username = session['username'], engine_no = engine_no, message = message)
            elif 'db-two' in request.form.keys():
                engine_no = 2
            return redirect(url_for('select_tables_for_engine', engine_no = engine_no))
            
@app.route('/tables/<int:engine_no>', methods = ['GET'])
def select_tables_for_engine(engine_no:int):
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    elif request.method == 'GET':
        if not engine_no == 1 and not engine_no == 2:
            return None
        if engine_no == 1:
            engine = engine_1
        elif engine_no == 2:
            engine = engine_2
        tables, previews = list_all_tables_in_db_with_preview(engine)
        db_name = engine.url.database
        message = f'Verbindung zur Datenbank {db_name} aufgebaut.'
        flash(message)
        return render_template('tables.html', engine_no = engine_no, db_name = db_name, tables = tables, previews = previews, tables_in_use = tables_in_use, message = message)

        
@app.route('/tables', methods=['POST'])
def select_tables():
    if not session.get('logged_in') or (engine_1 == None and engine_2 == None):
        return redirect(url_for('start'))
    print(request)
    global tables_in_use
    global meta_data_table_1
    global meta_data_table_2
    engine_no = int(request.form['engine-no'])
    engine = None
    table_names = request.form.getlist('selected-table')
    if engine_no == 1:
        engine = engine_1
    elif engine_no == 2:
        engine = engine_2
    for table_name in table_names:
        primary_keys = get_primary_key_from_engine(engine, table_name)
        data_type_info = check_data_type_meta_data(engine, table_name)
        column_names_and_data_types = get_column_names_data_types_and_max_length(engine, table_name)
        total_row_count = get_row_count_from_engine(engine, table_name)
        if tables_in_use == 0:
            tables_in_use += 1
            meta_data_table_1 = TableMetaData(engine, table_name, primary_keys, data_type_info, column_names_and_data_types, total_row_count)
            columns = meta_data_table_1.columns
        elif tables_in_use == 1:
            tables_in_use += 2
            meta_data_table_2 = TableMetaData(engine, table_name, primary_keys, data_type_info, column_names_and_data_types, total_row_count)
            columns = meta_data_table_2.columns
        else:
            message = 'Sie haben bereits zwei Tabellen ausgewählt.'
            return redirect(url_for('compare_two_dbs'))
            
    if tables_in_use == 1:
        if 'second-db-checkbox' in request.form.keys():
            message = f'Tabelle {meta_data_table_1.table_name} aus der Datenbank {engine.url.database} ausgewählt.'
            flash(message)
            return redirect(url_for('show_db_login_page', engine_no = 2))
        else:
            data = get_full_table_ordered_by_primary_key(meta_data_table_1)
            #TODO: replace with search.html
            return render_template('one-table.html', table_name = table_name, table_columns = columns, data = data)  
    elif tables_in_use == 3:
        return redirect(url_for('compare_two_dbs'))
        
        
    
    
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
            message = ''
            if not login_result == 0:
                if login_result == 1:
                    message = 'Sie haben sich bereits mit zwei Datenbanken verbunden.'
                elif login_result == 2:
                    message = 'Beim Aufbau der Datenbankverbindung ist ein Fehler aufgetreten. Bitte versuchen Sie es erneut.'
                return render_template('singledb.html', username = session['username'], message = message)
            else:
                if 'onedb' in request.form.keys():
                    tables = dict()
                    engine_no = 0
                    if db_in_use == 1:
                        tables, previews = list_all_tables_in_db_with_preview(engine_1)
                        engine_no = 1
                    elif db_in_use == 2:
                        tables, previews = list_all_tables_in_db_with_preview(engine_2)
                        engine_no = 2
                    return render_template('tables.html', engine_no = engine_no, db_name = db_name, tables = tables, previews = previews, message = message)
                elif 'twodbs' in request.form.keys():
                    return render_template('tables.html')
        elif request.method == 'GET':
            return render_template('singledb.html', username = session['username'])
   
@app.route('/search', methods = ['POST', 'GET'] )
def search_entries():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    db_name = engine_1.url.database
    table_name = meta_data_table_1.table_name
    table_columns = meta_data_table_1.columns
    searched_string = ''
    full_table = get_full_table_ordered_by_primary_key(meta_data_table_1)
    data = []
    if request.method == 'GET':
        data = full_table
    elif request.method == 'POST':
        column_name = request.form['column-to-search']
        if column_name == 'all':
            column_names = meta_data_table_1.columns
        else:
            column_names = [column_name]
        string_to_search = request.form['search-string']
        if string_to_search == '':
            data = full_table
        else:
            data = search_string(meta_data_table_1, string_to_search, column_names)
        searched_string = string_to_search
    return render_template('search.html', db_name = db_name, table_name = table_name, searched_string = searched_string, table_columns = table_columns, data = data)

@app.route('/replace', methods = ['GET', 'POST'])
def search_and_replace_entries():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    db_name = engine_1.url.database
    table_name = meta_data_table_1.table_name
    table_columns = meta_data_table_1.columns
    primary_keys = meta_data_table_1.primary_keys
    if request.method == 'GET':
        data = get_full_table_ordered_by_primary_key(meta_data_table_1)
        return render_template('replace.html', db_name = db_name, table_name = table_name, table_columns = table_columns, data = data)
    elif request.method == 'POST':
        string_to_replace = request.form['string-to-replace']
        replacement_string = request.form['replacement-string']
        column_names = request.form['affected-attributes'].removeprefix('[').removesuffix(']').replace('\'', '').split(', ')
        print('Spalten: ', column_names)
        affected_occurrences = request.form.getlist('selection')
        global replacement_occurrence_dict
        total_occurrences = len(replacement_occurrence_dict)
        # Erstellen einer Kopie des Dictionarys mit den Vorkommen des gesuchten Wertes, damit die nicht ausgewählten Werte hieraus entfernt werden können
        occurrences_to_change = replacement_occurrence_dict.copy()
        for key in replacement_occurrence_dict.keys():
            if str(key) not in affected_occurrences:
                occurrences_to_change.pop(key)
        print(len(occurrences_to_change))
        if len(occurrences_to_change) == total_occurrences:
            try:
                data = replace_all_string_occurrences(meta_data_table_1, column_names, string_to_replace, replacement_string, commit=True)
            except Exception as error:
                message = str(error)
            else:
                message = f'Alle {total_occurrences} Ersetzungen wurden erfolgreich vorgenommen.'
        elif len(occurrences_to_change) == 0:
            data = get_full_table_ordered_by_primary_key(meta_data_table_1)
            message = 'Es wurden keine Einträge ausgewählt, daher wurde nichts verändert.'
        else:
            # Einfügen der Primärschlüsselattribute in das Dictionary der einzufügenden Werte, damit diese an die Funktion replace_some_string_occurrences
            # übergeben werden können. Der Zähler für die Vorkommen des gesuchten Wertes beginnt bei 1.
            occurrences_to_change[0] = {'primary_keys': meta_data_table_1.primary_keys}
            message = replace_some_string_occurrences(meta_data_table_1, occurrences_to_change, string_to_replace, replacement_string, commit=True)
            data = get_full_table_ordered_by_primary_key(meta_data_table_1)
        replacement_occurrence_dict = None
        return render_template('replace.html', db_name = db_name, table_name = table_name, table_columns = table_columns, data = data, message = message)

@app.route('/replace-preview', methods = ['GET', 'POST'])
def select_entries_to_update():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    db_name = engine_1.url.database
    table_name = meta_data_table_1.table_name
    table_columns = meta_data_table_1.columns
    primary_keys = meta_data_table_1.primary_keys
    # /replace-preview kann nicht über GET-Requests aufgerufen werden
    if request.method == 'GET':
        data = get_full_table_ordered_by_primary_key(meta_data_table_1, convert=False)
        return render_template('replace.html', db_name = db_name, table_name = table_name, table_columns = table_columns, data = data)
    elif request.method == 'POST':
        # neu einzusetzender String
        input = request.form['replacement']
        # zu durchsuchendes Attribut der Datenbank, 'all', wenn alle Spalten durchsucht werden sollen
        attribute_to_search = request.form['columntosearch']
        # zu ersetzender String
        string_to_search = request.form['searchstring']
        # Primärschlüssel der Tabelle
        primary_keys = meta_data_table_1.primary_keys
        # Anlegen der Liste der betroffenen Attribute für die Übergabe an das Model
        affected_attributes = []
        # Anlegen einer Liste, mit der die Kompatibilität des Inputs mit den Datentypen der Spalten überprüft wird
        attribute_list = []
        # Erstellen eines Dictionarys mit den Spaltennamen als Schlüssel und einer Liste als Wert, die den Datentypen, dessen Einordnung in Strings
        # (Wert 0), ganze Zahlen (Wert 1), Kommazahlen (Wert 2) und 'andere' (Wert 3) sowie die maximale Zeichenanzahl (None für nicht textbasierte
        # Datentypen) enthält
        cols_dtypes_and_num_types = get_column_names_data_types_and_max_length(engine_1, table_name)
        # Nachricht, die auf der Webseite angezeigt wird; leer, solange keine Fehler auftreten oder Warnungen nötig sind
        message = ''

        ### Grobe Überprüfung, mit welchen Datentypen der Spalten der neue Wert kompatibel ist ###
        # Wenn alle Spalten durchsucht werden sollen ...
        if attribute_to_search == 'all':
            # ... steht in attribute_list für jede Spalte der aktuellen Tabelle der Wert 1 ...
            attribute_list = [1] * len(table_columns)
            # ... und eine Kopie der Liste mit allen Attributen wird für die Datenbankabfrage benötigt.
            affected_attributes = table_columns.copy()
        # Anderenfalls wird nur ein Attribut durchsucht.
        else:
            # Dieses wird in eine Liste eingetragen, weil replace_all_string_occurrences die Attribute als Liste erwartet.
            affected_attributes = [attribute_to_search]
            # attribute_list wird als Liste mit so vielen Nullen initialisiert, wie die aktuelle Tabelle Spalten hat ...
            attribute_list = [0] * len(table_columns)
            for index, attribute in enumerate(table_columns):
                if attribute == attribute_to_search:
                    # ... und an die Position des zu durchsuchenden Attributs wird der Wert 1 geschrieben.
                    attribute_list[index] = 1
                    # Da nur eine Spalte durchsucht wird, kann die Schleife anschließend abgebrochen werden.
                    break
        print('Before: ', attribute_list)
        validity_list = [None]* len(attribute_list)
        # Nun wird diese Liste mit entweder nur Einsen oder nur Nullen und einer Eins durchiteriert ...       
        for index, attribute in enumerate(attribute_list):
            print('spalten: ', table_columns)
            print('Attribute: ', affected_attributes)
            # ... und für jede Eins, d. h. jedes betroffene Attribut ...
            if attribute:
                # ... überprüft, ob der im Browser eingegebene String als der entsprechende Datentyp (int oder float) interpretiert werden kann
                # bzw. die maximale Zeichenanzahl nicht überschreitet, wenn es sich um einen textbasierten Datentyp handelt.
                validity = check_validity_of_input_and_searched_value(meta_data_table_1, input, table_columns[index], string_to_search)
                # Ist die Überprüfung bestanden, gibt die Funktion einene leeren String aus, bei Fehlern eine entsprechende Meldung.
                # Wenn eine Fehlermeldung besteht ...
                print('Überprüfung ergibt:', validity)
                validity_list[index] = validity
                if validity != 0:
                    
                    # ... wird die Eins des betroffenen Attributs in attribute_list auf den Wert 0 gesetzt, ...
                    attribute_list[index] = 0
                    # ... das betroffene Attribut aus der Liste der zu durchsuchenden Attribute entfernt.
                    attribute_to_ignore = table_columns[index]
                    affected_attributes.remove(attribute_to_ignore)
        # Volle Tabelle vor der Änderung
        unchanged_data = get_full_table_ordered_by_primary_key(meta_data_table_1)
        # Wenn nach dem Durchlaufen dieser Schleife nur Nullen in dieser Liste stehen, ist der eingegebene Wert mit keinem der zu durchsuchenden
        # Attribute kompatibel oder in den kompatiblen Spalten wurden keine passenden Einträge gefunden, ...
        print(attribute_list)
        if sum(attribute_list) == 0:
            # ... daher wird dies als Fehlermeldung ausgegeben ...
            message = f'Der eingegebene Wert \'{input}\' ist nicht mit allen Datentypen und/oder Constraints der ausgewählten Spalten kompatibel und in den verbleibenden Spalten wurden keine passenden Einträge gefunden. Bitte versuchen Sie es erneut.'
            # ... und der Nutzer wird zur Startseite der Suchen-und-Ersetzen-Funktion weitergeleitet.
            return render_template('replace.html', db_name = db_name, table_name = table_name, table_columns = table_columns, data = unchanged_data, message = message)
        # Steht noch mindestens eine Eins in attribute_list, ist der eingegebene Wert mit mindestens einem durchsuchten Attribut kompatibel, jedoch
        # wurden manche Attribute aus der Suche ausgeschlossen. Hier wird ermittelt, welche Nachricht/Fehlermeldung hierzu ausgegeben werden soll.
        elif sum(attribute_list) < len(attribute_list):
            message = ''
            for index, error_code in enumerate(validity_list):
                if error_code == None:
                    continue
                elif error_code == 1:
                    message += f"\nDer eingegebene Wert \'{input}\' kann nicht in den Datentyp {cols_dtypes_and_num_types[table_columns[index]]['data_type']} des Attributs {table_columns[index]} umgewandelt werden."
                elif error_code == 2:
                    message += f"\nDer gesuchte Wert \'{string_to_search}\' entspricht nicht dem Datentyp {cols_dtypes_and_num_types[table_columns[index]]['data_type']} des Attributs {table_columns[index]}."
                elif error_code == 3:
                    message += f"\nDer eingegebene Wert \'{input}\' überschreitet die maximal erlaubte Zeichenanzahl {cols_dtypes_and_num_types[table_columns[index]]['char_max_length']} des Attributs {table_columns[index]}"
                elif error_code == 4:
                    message += f'\nDer gesuchte Wert \'{string_to_search}\' kommt im Attribut {table_columns[index]} nicht vor.'
                elif error_code == 5:
                    message += f'\nDer eingegebene Wert \'{input}\' verletzt eine \'Unique\'-Constraint des Attributs {table_columns[index]}.'
                elif error_code == 6:
                    message += f'\nDer eingegebene Wert \'{input}\' verletzt eine Constraint des Attributs {table_columns[index]}.'
                elif error_code == 7:
                    message += f'\nBei der Abfrage des Attributs {table_columns[index]} ist ein Datenbankfehler aufgetreten.'
            if message != '':
                message = message.strip() + '\nDie betroffenen Attribute wurden aus der Suche ausgeschlossen.'

        ### Simulation des Ersetzungsvorgangs zum Erstellen der Vorschau ###
        
        # Bilde Liste mit Tupeln aus den einzelnen Spaltennamen und 1, wenn der eingegebene Wert mit dem Datentyp kompatibel ist, 0, wenn nicht.
        attributes_and_positions = list(zip(table_columns, attribute_list))
        global replacement_data_dict
        global replacement_occurrence_dict
        # Versuche, die Änderungsdaten je Tabellenzeile (Zeilennummer, die alten Werte, die Änderungspositionen und die neuen Werte in 
        # replacement_data_dict) sowie je betroffener Tabellenzelle (Zeilennumer, Primärschlüssel, Spaltenname in replacement_occurrence_dict)
        # aus der Datenbank zu beziehen.
        try:
            replacement_data_dict, replacement_occurrence_dict = get_replacement_information(meta_data_table_1, attributes_and_positions, string_to_search, input) 
        # Falls dabei Fehler auftreten, ...
        except Exception as error:
            # raise error
            # ... wird die Fehlermeldung übernommen ...
            message = str(error)
            # ... und der Nutzer zur Startseite der Suchen-und-Ersetzen-Funktion weitergeleitet.
            return render_template('replace.html', db_name = db_name, table_name = table_name, table_columns = table_columns, data = unchanged_data, message = message)
        # Falls die Abfrage keine Ergebnisse liefert, ...
        else:
            # ... das Ergebnis jedoch leer ist, ...
            if len(replacement_data_dict.values()) == 0:
                # ... wird dies auf der Webseite angezeigt.
                message = 'Keine passenden Einträge gefunden.'
                return render_template('replace.html', db_name = db_name, table_name = table_name, table_columns = table_columns, data = unchanged_data, message = message)
            else:
                # Anderenfalls wird mithilfe der gewonnenen Daten unter localhost:8000/replace-preview die Vorschau erstellt.
                return render_template('replace-preview.html', db_name = db_name, table_name = table_name, occurrence_dict = replacement_occurrence_dict, table_columns = table_columns, string_to_replace = string_to_search, replacement_string = input, replacement_data_dict = replacement_data_dict, affected_attributes = affected_attributes, message = message)


@app.route('/unify', methods = ['GET', 'POST'])
def unify_db_entries():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    db_name = engine_1.url.database
    table_name = meta_data_table_1.table_name
    table_columns = meta_data_table_1.columns
    primary_keys = meta_data_table_1.primary_keys
    message = ''
    for item in request.args:
        print(item)
    if request.method == 'GET':
        data = get_full_table_ordered_by_primary_key(meta_data_table_1)
    elif request.method == 'POST':
        attribute_to_change = request.form['columntounify']
        # \ wird beim Auslesen zu \\ -> muss rückgängig gemacht werden, weil Parameter zweimal an HTML-Dokumente gesendet wird
        old_values = request.form['old-values'].replace('[', '').replace(']', '').replace('\'', '').replace('\\\\', '\\').split(', ')
        new_value = request.form['new-value']
        message = 'Änderungen erfolgreich durchgeführt.'
        try:
            update_to_unify_entries(meta_data_table_1, attribute_to_change, old_values, new_value, True)
        except Exception as error:
            message = str(error)
        data = get_full_table_ordered_by_primary_key(meta_data_table_1)
    return render_template('unify.html', db_name = db_name, table_columns = table_columns, primary_keys = primary_keys, data = data, table_name = table_name, engine_no = 1, message = message)
     
        

@app.route('/unify-selection', methods = ['GET', 'POST'])
def select_entries_to_unify():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    elif request.method == 'GET':
        return redirect(url_for('unify_db_entries'), code = 302)
    elif request.method == 'POST':
        db_name = engine_1.url.database
        table_name = request.form['table-name']
        column_to_unify = request.form['column-to-unify']
        data = get_unique_values_for_attribute(meta_data_table_1, column_to_unify)
        return render_template('unify-selection.html', db_name = db_name, table_name = table_name, column_to_unify = column_to_unify, data = data, engine_no = 1)

@app.route('/unify-preview', methods = ['GET', 'POST'])
def show_affected_entries():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    elif request.method == 'GET':
        return redirect(url_for('unify_db_entries'), code = 302)
    elif request.method == 'POST':
        print(request)
        db_name = engine_1.url.database
        table_name = meta_data_table_1.table_name
        table_columns = meta_data_table_1.columns
        cols_and_dtypes = meta_data_table_1.column_names_and_data_types
        column_to_unify = request.form['column-to-unify']
        print(column_to_unify)
        new_value = request.form['replacement']
        old_values = list()
        for key in request.form.keys():
            if re.match(r'^[0-9]+$', key):
                old_values.append(request.form[key])
        print(old_values)
        unique_values = get_unique_values_for_attribute(meta_data_table_1, column_to_unify)
        if len(old_values) < 1:
            data = unique_values
            message = 'Bitte wählen Sie mindestens ein zu bearbeitendes Attribut aus und versuchen Sie es erneut.'
            return render_template('unify-selection.html', db_name = db_name, table_name = table_name, column_to_unify = column_to_unify, data = data, engine_no = 1, message = message)
        message = ''
        validity = check_validity_of_input_and_searched_value(meta_data_table_1, new_value, column_to_unify, old_values[0])
        print(validity)
        if validity != 0:
            data = unique_values
            if validity == 1:
                message = f"Der eingegebene neue Wert \'{new_value}\' ist nicht mit dem Datentyp {cols_and_dtypes[column_to_unify]['data_type']} des Attributs {column_to_unify} kompatibel."
            elif validity == 3:
                message = f"\nDer eingegebene Wert \'{new_value}\' überschreitet die maximal erlaubte Zeichenanzahl {cols_and_dtypes[column_to_unify]['char_max_length']} des Attributs {column_to_unify}"
            elif validity == 5:
                message += f'\nDer eingegebene Wert \'{new_value}\' verletzt eine \'unique\'-Constraint des Attributs {column_to_unify}.'
            elif validity == 6:
                message += f'\nDer eingegebene Wert \'{new_value}\' verletzt eine Constraint des Attributs {column_to_unify}.'
            elif validity == 7:
                message += f'\nBei der Abfrage des Attributs {column_to_unify} ist ein Datenbankfehler aufgetreten.'
            message = message + ' Bitte versuchen Sie es erneut.'
            return render_template('unify-selection.html', db_name = db_name, table_name = table_name, column_to_unify = column_to_unify, data = data, engine_no = 1, message = message)
        else:
            try:
                update_to_unify_entries(meta_data_table_1, column_to_unify, old_values, new_value, False)
            except Exception as error:
                message = str(error)
                if 'constraint' in message.lower():
                    message = 'Der eingegebene neue Wert verletzt eine Bedingung (Constraint) Ihrer Datenbank. Bitte versuchen Sie es erneut.'
                data = unique_values
                return render_template('unify-selection.html', db_name = db_name, table_name = table_name, column_to_unify = column_to_unify, data = data, engine_no = 1, message = message)
        
        index_of_affected_attribute = 0
        
        data = get_full_table_ordered_by_primary_key(meta_data_table_1)
        affected_entries = get_row_number_of_affected_entries(meta_data_table_1, [column_to_unify], old_values, mode = 'unify')
        affected_rows = []
        for row in affected_entries:
            affected_rows.append(row[0])
        row_total = meta_data_table_1.total_row_count
        for index, column in enumerate(table_columns):
            if column == column_to_unify:
                index_of_affected_attribute = index + 1
                break
        
        return render_template('unify-preview.html', db_name = db_name, table_name = table_name, table_columns = table_columns, column_to_unify = column_to_unify, old_values = old_values, new_value = new_value, data = data, index_of_affected_attribute = index_of_affected_attribute, affected_rows = affected_rows, row_total = row_total)


@app.route('/disconnect')
def disconnect_from_single_db():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    global engine_1
    global db_in_use
    global tables_in_use
    global meta_data_table_1

    engine_1 = None
    db_in_use = 0
    tables_in_use = 0
    meta_data_table_1 = None
    
    return redirect(url_for('show_db_login_page', engine_no = 1))


@app.route('/compare', methods = ['GET', 'POST'])
def compare_two_dbs():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    if request.method == 'GET':
        global basic_compatibility_list
        basic_compatibility_list, compatibility_by_code = check_basic_data_type_compatibility(meta_data_table_1, meta_data_table_2, True)
        return show_both_tables_separately(compatibility_by_code)
    elif request.method == 'POST':
        target_table = request.form['target-table']
        join_attribute_string = request.form['attribute-selection'] #.removeprefix('(').removesuffix(')').replace("\'", '')
        cast_direction = request.form['cast']
        join_attributes = join_attribute_string.split(', ')
        table_meta_data_for_join_1 = None
        table_meta_data_for_join_2 = None
        selected_columns_table_1 = []
        selected_columns_table_2 = [] 
        attributes_to_join_on = []
        if target_table != 'table_1':
            table_meta_data_for_join_1 = meta_data_table_2
            table_meta_data_for_join_2 = meta_data_table_1
            selected_columns_table_1 = request.form.getlist('columns-table2')
            selected_columns_table_2 =  request.form.getlist('columns-table1')
            helper_attribute = join_attributes[0]
            join_attributes[0] = join_attributes[1]
            join_attributes[1] = helper_attribute 
            if cast_direction == 1:
                cast_direction = 2
            elif cast_direction == 2:
                cast_direction = 1

        else:
            table_meta_data_for_join_1 = meta_data_table_1
            table_meta_data_for_join_2 = meta_data_table_2
            selected_columns_table_1 =  request.form.getlist('columns-table1')
            selected_columns_table_2 = request.form.getlist('columns-table2')
        meta_data_list = [table_meta_data_for_join_1, table_meta_data_for_join_2]
                
        cast_direction = int(request.form.get('cast'))
        for attribute in join_attributes:
            attributes_to_join_on.append(attribute)
        dialect_1 = table_meta_data_for_join_1.engine.dialect.name
        dialect_2 = table_meta_data_for_join_2.engine.dialect.name
        url_1 = table_meta_data_for_join_1.engine.url
        url_2 = table_meta_data_for_join_2.engine.url
        if url_1 == url_2 or (dialect_1 == 'mariadb' and dialect_2 == 'mariadb' and url_1.host == url_2.host and url_1.port == url_2.port):
            try:
                data, column_names, unmatched_rows = join_tables_of_same_dialect_on_same_server(meta_data_list, attributes_to_join_on, selected_columns_table_1, selected_columns_table_2, cast_direction)
            except Exception as error:
                flash(str(error))
                return redirect(url_for('compare_two_dbs'))
        else:
            try:
                data, column_names, unmatched_rows = join_tables_of_different_dialects_dbs_or_servers(meta_data_list, attributes_to_join_on, selected_columns_table_1, selected_columns_table_2, cast_direction)
            except Exception as error:
                flash(str(error))
                return redirect(url_for('compare_two_dbs'))
        db_name_1 = table_meta_data_for_join_1.engine.url.database
        db_name_2 = table_meta_data_for_join_2.engine.url.database
        dialects = [dialect_1, dialect_2]
        table_name_1 = table_meta_data_for_join_1.table_name
        table_name_2 = table_meta_data_for_join_2.table_name
        
        return render_template('joined-preview.html', db_name_1 = db_name_1, db_name_2 = db_name_2, table_name_1 = table_name_1, table_name_2 = table_name_2, db_dialects = dialects, join_attribute_1 = join_attributes[0], join_attribute_2 = join_attributes[1], table_columns = column_names, data = data, unmatched_rows = unmatched_rows)


@app.route('/merge', methods = ['GET', 'POST'])
def merge_tables():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    if request.method == 'GET':
        pass
    elif request.method == 'POST':
        target_table = request.form['target-table']
        if target_table == 'table_1':
            source_table_data = meta_data_table_2
            target_table_data = meta_data_table_1
        else: 
            source_table_data = meta_data_table_1
            target_table_data = meta_data_table_2
       
    
@app.route('/logout', methods = ['GET', 'POST'])
def logout():
    if not session.get('logged_in') or request.method == 'GET':
        return redirect(url_for('start'))
    elif request.method == 'POST':
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
    status = 0
    if engine_1 and engine_2:
        status = 1
    else:
        try: 
            db_engine = connect_to_db(username, password, host, port, db_name, db_dialect, db_encoding)
        except DatabaseError:
            status = 2
        else:
            if not engine_1:
                engine_1 = db_engine
                db_in_use += 1
            elif engine_1 and not engine_2:
                engine_2 = db_engine
                db_in_use += 2
    return status

    
def show_both_tables_separately(compatibility_by_code:dict):
    db_name_1 = meta_data_table_1.engine.url.database
    db_name_2 = meta_data_table_2.engine.url.database
    db_dialects = []
    db_dialects.append(meta_data_table_1.engine.dialect.name)
    db_dialects.append(meta_data_table_2.engine.dialect.name)
    for index, dialect in enumerate(db_dialects):
        if dialect == 'mariadb':
            db_dialects[index] = 'MariaDB'
        elif dialect == 'postgresql':
            db_dialects[index] = 'PostgreSQL'
    table_1 = meta_data_table_1.table_name
    table_2 = meta_data_table_2.table_name
    data_1 = get_full_table_ordered_by_primary_key(meta_data_table_1)
    data_2 = get_full_table_ordered_by_primary_key(meta_data_table_2)
    table_columns_1 = meta_data_table_1.columns
    table_columns_2 = meta_data_table_2.columns
    return render_template('compare.html', db_name_1 = db_name_1, db_dialects = db_dialects, table_name_1 = table_1, db_name_2 = db_name_2, table_name_2 = table_2, 
                               table_columns_1 = table_columns_1, data_1 = data_1, table_columns_2 = table_columns_2, data_2 = data_2, comp_by_code = compatibility_by_code)


if __name__ == '__main__':
    
    print(len((False, 0)))
    app.secret_key = os.urandom(12)
    serve(app, host = '0.0.0.0', port = 8000)
    