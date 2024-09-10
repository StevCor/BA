from flask import Flask, render_template, request, flash, redirect, session, url_for
import os
import re
from waitress import serve
from ControllerClasses import TableMetaData
from controllerFunctions import check_validity_of_input_and_searched_value, show_both_tables_separately, update_TableMetaData_entries
from model.SQLDatabaseError import DatabaseError, DialectError
from model.loginModel import register_new_user, login_user 
from model.databaseModel import check_data_type_meta_data, connect_to_db, convert_result_to_list_of_lists, get_primary_key_from_engine, get_row_count_from_engine, list_all_tables_in_db_with_preview, get_full_table_ordered_by_primary_key
from model.oneTableModel import get_replacement_information, get_row_number_of_affected_entries, get_unique_values_for_attribute, replace_all_string_occurrences, replace_some_string_occurrences, search_string, update_to_unify_entries
from model.twoTablesModel import check_basic_data_type_compatibility, execute_merge_and_add_constraints, join_tables_of_different_dialects_dbs_or_servers, join_tables_of_same_dialect_on_same_server, simulate_merge_and_build_query

# globale Variablen für den Datenbankzugriff
global engine_1
global engine_2
global db_in_use
global tables_in_use
global meta_data_table_1
global meta_data_table_2
engine_1 = None
engine_2 = None
db_in_use = 0 # 0: keine Engine, 1: engine_1 ist nutzbar, 2: engine_2 ist nutzbar, 3: engine_ und engine_2 sind nutzbar
tables_in_use = 0 # 0: keine Tabelle ausgewählt, 1: eine Tabelle ausgewählt, 3: zwei Tabellen ausgewählt

# globale Variablen für die Identifizierung der String-Vorkommen, die in 'Suchen und Ersetzen' aktualisiert werden sollen
global replacement_occurrence_dict
global replacement_data_dict

# globales Dictionary für die Kompatibilitätsprüfung zur Anzeige der Operationen auf zwei Tabellen
global compatibility_by_code
compatibility_by_code = None

# globale Variablen für die Ausführung der Attributsübertragung zwischen zwei Tabellen
global source_attribute
global target_attribute
global query_parameters
global merge_query
source_attribute = None
target_attribute = None
query_parameters = None
merge_query = None

# Erstellen der Flask-Anwendung, die in __main__ gestartet wird; Festlegung der Ordner für die HTML-Dateien (template_folder) sowie JavaScript und CSS (static_folder)
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
            flash(result[1])
    return render_template('login.html')
    
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
        global engine_1
        global engine_2
        global db_in_use
        if engine_1 and engine_2:
            flash('Sie haben sich bereits mit zwei Datenbanken verbunden.')
            return redirect(url_for('compare_two_tables'))
        else:
            try: 
                db_engine = connect_to_db(username, password, host, port, db_name, db_dialect, db_encoding)
            except (DatabaseError, DialectError) as error:
                flash(str(error))
                message = str(error)
                return render_template('db-connect.html', username = session['username'], engine_1 = engine_1, engine_no = engine_no, message = message)
            else:
                if 'db-one' in request.form.keys():
                    engine_no = 1
                    if engine_1 is not None and engine_2 is not None:
                        return render_template('db-connect.html', username = session['username'], engine_1 = engine_1, engine_no = 2, message = '')
                    else:
                        engine_1 = db_engine
                        db_in_use += 1
                elif 'db-two' in request.form.keys():
                    engine_no = 2
                    if engine_1 is None:
                        return render_template('db-connect.html', username = session['username'], engine_1 = engine_1, engine_no = 1, message = '')
                    elif engine_1 is not None and engine_1.url == db_engine.url:
                        engine_2 = None
                        message = 'Sie haben sich bereits eine Tabelle aus dieser Datenbank ausgesucht. Bitte verbinden Sie sich mit einer anderen.'
                        flash(message)
                        return render_template('db-connect.html', username = session['username'], engine_1 = engine_1, engine_no = engine_no, message = message)
                    else:
                        engine_2 = db_engine
                        db_in_use += 2
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
        tables, previews, tables_without_keys = list_all_tables_in_db_with_preview(engine)
        db_name = engine.url.database
        message = f'Verbindung zur Datenbank {db_name} aufgebaut.'
        flash(message)
        return render_template('tables.html', engine_no = engine_no, db_name = db_name, tables = tables, previews = previews, tables_in_use = tables_in_use, tables_without_keys = tables_without_keys, message = message)

        
@app.route('/tables', methods=['POST'])
def select_tables():
    if not session.get('logged_in') or (engine_1 == None and engine_2 == None):
        return redirect(url_for('start'))
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
        total_row_count = get_row_count_from_engine(engine, table_name)
        if tables_in_use == 0:
            tables_in_use += 1
            meta_data_table_1 = TableMetaData(engine, table_name, primary_keys, data_type_info, total_row_count)
            columns = meta_data_table_1.columns
        elif tables_in_use == 1:
            tables_in_use += 2
            meta_data_table_2 = TableMetaData(engine, table_name, primary_keys, data_type_info, total_row_count)
            columns = meta_data_table_2.columns
        else:
            message = 'Sie haben bereits zwei Tabellen ausgewählt.'
            return redirect(url_for('compare_two_tables'))
            
    if tables_in_use == 1:
        if 'second-db-checkbox' in request.form.keys():
            message = f'Tabelle {meta_data_table_1.table_name} aus der Datenbank {engine.url.database} ausgewählt.'
            flash(message)
            return redirect(url_for('show_db_login_page', engine_no = 2))
        else:
            db_name = meta_data_table_1.engine.url.database
            data = get_full_table_ordered_by_primary_key(meta_data_table_1)
            return render_template('search.html', db_name = db_name, table_name = table_name, table_columns = columns, data = data, searched_string = '')  
    elif tables_in_use == 3:
        return redirect(url_for('compare_two_tables'))
        
        
   
@app.route('/search', methods = ['POST', 'GET'] )
def search_entries():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    if tables_in_use == 2:
        return redirect(url_for('compare_two_tables'))
    elif tables_in_use == 0:
        return redirect(url_for('set_up_db_connection'))
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
        affected_occurrences = request.form.getlist('selection')
        global replacement_occurrence_dict
        total_occurrences = len(replacement_occurrence_dict)
        # Erstellen einer Kopie des Dictionarys mit den Vorkommen des gesuchten Wertes, damit die nicht ausgewählten Werte hieraus entfernt werden können
        occurrences_to_change = replacement_occurrence_dict.copy()
        for key in replacement_occurrence_dict.keys():
            if str(key) not in affected_occurrences:
                occurrences_to_change.pop(key)
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
        flash(message)
        return render_template('replace.html', db_name = db_name, table_name = table_name, table_columns = table_columns, data = data)

@app.route('/replace-preview', methods = ['GET', 'POST'])
def select_entries_to_update():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    db_name = engine_1.url.database
    table_name = meta_data_table_1.table_name
    table_columns = meta_data_table_1.columns
    # /replace-preview kann nicht über GET-Requests aufgerufen werden
    if request.method == 'GET':
        data = get_full_table_ordered_by_primary_key(meta_data_table_1, convert = False)
        return render_template('replace.html', db_name = db_name, table_name = table_name, table_columns = table_columns, data = data)
    elif request.method == 'POST':
        # neu einzusetzender String
        input = request.form['replacement']
        # zu durchsuchendes Attribut der Datenbank, 'all', wenn alle Spalten durchsucht werden sollen
        attribute_to_search = request.form['column-to-search']
        # zu ersetzender String
        string_to_search = request.form['searchstring']
        # Anlegen der Liste der betroffenen Attribute für die Übergabe an das Model
        affected_attributes = []
        # Anlegen einer Liste, mit der die Kompatibilität des Inputs mit den Datentypen der Spalten überprüft wird
        attribute_list = []
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
        message = ''
        # Nun wird die Attributliste mit entweder nur Einsen oder nur Nullen und einer Eins durchiteriert ...       
        for index, attribute in enumerate(attribute_list):
            print('spalten: ', table_columns)
            print('Attribute: ', affected_attributes)
            # ... und für jede Eins, d. h. jedes betroffene Attribut ...
            if attribute:
                # ... überprüft, ob der im Browser eingegebene String als der entsprechende Datentyp (int oder float) interpretiert werden kann
                # bzw. die maximale Zeichenanzahl nicht überschreitet, wenn es sich um einen textbasierten Datentyp handelt.
                validity = check_validity_of_input_and_searched_value(meta_data_table_1, input, table_columns[index], string_to_search)
                # Ist die Überprüfung bestanden, gibt die Funktion den Wert 0 aus, bei Fehlern eine entsprechende Meldung.
                # Wenn eine Fehlermeldung besteht ...
                if validity != 0:
                    # ... wird die Eins des betroffenen Attributs in attribute_list auf den Wert 0 gesetzt, ...
                    attribute_list[index] = 0
                    # ... das betroffene Attribut aus der Liste der zu durchsuchenden Attribute entfernt ...
                    attribute_to_ignore = table_columns[index]
                    affected_attributes.remove(attribute_to_ignore)
                    # ... und die Fehlermeldung zur späteren Ausgabe gespeichert.
                    message += validity
        # Volle Tabelle vor der Änderung
        unchanged_data = get_full_table_ordered_by_primary_key(meta_data_table_1)
        # Wenn nach dem Durchlaufen dieser Schleife nur Nullen in dieser Liste stehen, ist der eingegebene Wert mit keinem der zu durchsuchenden
        # Attribute kompatibel oder in den kompatiblen Spalten wurden keine passenden Einträge gefunden, ...
        if sum(attribute_list) == 0:
            # ... daher wird dies als Fehlermeldung ausgegeben ...
            flash(f'Der eingegebene Wert \'{input}\' ist nicht mit allen Datentypen und/oder Constraints der ausgewählten Spalten kompatibel und in den verbleibenden Spalten wurden keine passenden Einträge gefunden. Bitte versuchen Sie es erneut.')
            # ... und der Nutzer wird zur Startseite der Suchen-und-Ersetzen-Funktion weitergeleitet.
            return render_template('replace.html', db_name = db_name, table_name = table_name, table_columns = table_columns, data = unchanged_data)
        # Steht noch mindestens eine Eins in attribute_list, ist der eingegebene Wert mit mindestens einem durchsuchten Attribut kompatibel, jedoch
        # wurden manche Attribute aus der Suche ausgeschlossen. Hier wird ermittelt, welche Nachricht/Fehlermeldung hierzu ausgegeben werden soll.
        elif sum(attribute_list) < len(attribute_list):
            if message != '':
                message += 'Die betroffenen Attribute wurden aus der Suche ausgeschlossen.'

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
             raise error
            # ... wird die Fehlermeldung übernommen ...
           # flash(str(error))
            # ... und der Nutzer zur Startseite der Suchen-und-Ersetzen-Funktion weitergeleitet.
            #return render_template('replace.html', db_name = db_name, table_name = table_name, table_columns = table_columns, data = unchanged_data)
        # Falls keine Fehler auftreten, ...
        else:
            # ... das Ergebnis jedoch leer ist, ...
            if len(replacement_data_dict.values()) == 0:
                # ... wird dies auf der Webseite angezeigt.
                flash('Keine passenden Einträge gefunden.')
                return render_template('replace.html', db_name = db_name, table_name = table_name, table_columns = table_columns, data = unchanged_data)
            else:
                # Anderenfalls werden die Meldungen für den Ausschluss von Attributen angezeigt
                flash(message)
                # und mithilfe der gewonnenen Daten wird unter localhost:8000/replace-preview die Vorschau erstellt.
                return render_template('replace-preview.html', db_name = db_name, table_name = table_name, occurrence_dict = replacement_occurrence_dict, table_columns = table_columns, string_to_replace = string_to_search, replacement_string = input, replacement_data_dict = replacement_data_dict, affected_attributes = affected_attributes)


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
        attribute_to_change = request.form['column-to-unify']
        # \ wird beim Auslesen zu \\ -> muss rückgängig gemacht werden, weil Parameter zweimal an HTML-Dokumente gesendet wird
        old_values = request.form['old-values'].replace('[', '').replace(']', '').replace('\'', '').replace('\\\\', '\\').split(', ')
        new_value = request.form['new-value']
        message = 'Änderungen erfolgreich durchgeführt.'
        try:
            update_to_unify_entries(meta_data_table_1, attribute_to_change, old_values, new_value, True)
        except Exception as error:
            message = str(error)
        data = get_full_table_ordered_by_primary_key(meta_data_table_1)
        flash(message)
    return render_template('unify.html', db_name = db_name, table_columns = table_columns, primary_keys = primary_keys, data = data, table_name = table_name, engine_no = 1)    

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
        db_name = engine_1.url.database
        table_name = meta_data_table_1.table_name
        table_columns = meta_data_table_1.columns
        column_to_unify = request.form['column-to-unify']
        print(column_to_unify)
        new_value = request.form['replacement']
        if new_value in ('', 'None', 'NULL'):
            new_value = None
        old_values = list()
        for key in request.form.keys():
            if re.match(r'^[0-9]+$', key):
                old_values.append(request.form[key])
        unique_values = get_unique_values_for_attribute(meta_data_table_1, column_to_unify)
        if len(old_values) < 1:
            data = unique_values
            flash('Bitte wählen Sie mindestens ein zu bearbeitendes Attribut aus und versuchen Sie es erneut.')
            return render_template('unify-selection.html', db_name = db_name, table_name = table_name, column_to_unify = column_to_unify, data = data, engine_no = 1)
        validity = check_validity_of_input_and_searched_value(meta_data_table_1, new_value, column_to_unify, old_values[0])
        if validity != 0:
            flash(validity)
            return render_template('unify-selection.html', db_name = db_name, table_name = table_name, column_to_unify = column_to_unify, data = data, engine_no = 1)
        else:
            try:
                update_to_unify_entries(meta_data_table_1, column_to_unify, old_values, new_value, False)
            except Exception as error:
                if 'constraint' in str(error).lower():
                    flash('Der eingegebene neue Wert verletzt eine Bedingung (Constraint) Ihrer Datenbank. Bitte versuchen Sie es erneut.')
                data = unique_values
                return render_template('unify-selection.html', db_name = db_name, table_name = table_name, column_to_unify = column_to_unify, data = data, engine_no = 1)
        
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

@app.route('/compare', methods = ['GET', 'POST'])
def compare_two_tables():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    if tables_in_use != 3:
        if tables_in_use == 0:
            engine_no = 1
        elif tables_in_use == 1:
            engine_no = 2
        return redirect(url_for('show_db_login_page', engine_no = engine_no))
    if request.method == 'GET':
        global compatibility_by_code
        compatibility_by_code = check_basic_data_type_compatibility(meta_data_table_1, meta_data_table_2)
        return show_both_tables_separately(meta_data_table_1, meta_data_table_2, compatibility_by_code, 'compare')
    elif request.method == 'POST':
        target_table = request.form['target-table']
        join_attribute_string = request.form['attribute-selection']
        join_attributes = join_attribute_string.split(', ')
        cast_direction = int(request.form.get('cast'))
        full_outer_join = False
        if 'full-outer-join' in request.form.keys():
            full_outer_join = True
        if target_table != 'table_1':
            table_meta_data_for_join_1 = meta_data_table_2
            table_meta_data_for_join_2 = meta_data_table_1
            selected_columns_table_1 = request.form.getlist('columns-table2')
            selected_columns_table_2 =  request.form.getlist('columns-table1')
            attributes_to_join_on = [join_attributes[1], join_attributes[0]]
            if cast_direction == 1:
                cast_direction = 2
            elif cast_direction == 2:
                cast_direction = 1

        else:
            table_meta_data_for_join_1 = meta_data_table_1
            table_meta_data_for_join_2 = meta_data_table_2
            selected_columns_table_1 =  request.form.getlist('columns-table1')
            selected_columns_table_2 = request.form.getlist('columns-table2')
            attributes_to_join_on = join_attributes
        meta_data_list = [table_meta_data_for_join_1, table_meta_data_for_join_2]
                
        dialect_1 = table_meta_data_for_join_1.engine.dialect.name
        dialect_2 = table_meta_data_for_join_2.engine.dialect.name
        url_1 = table_meta_data_for_join_1.engine.url
        url_2 = table_meta_data_for_join_2.engine.url
        if url_1 == url_2 or (dialect_1 == 'mariadb' and dialect_2 == 'mariadb' and url_1.host == url_2.host and url_1.port == url_2.port):
            try:
                data, column_names, unmatched_rows = join_tables_of_same_dialect_on_same_server(meta_data_list, attributes_to_join_on, selected_columns_table_1, selected_columns_table_2, cast_direction, full_outer_join)
            except Exception as error:
                flash(str(error))
                return redirect(url_for('compare_two_tables'))
        else:
            try:
                data, column_names, unmatched_rows = join_tables_of_different_dialects_dbs_or_servers(meta_data_list, attributes_to_join_on, selected_columns_table_1, selected_columns_table_2, cast_direction, full_outer_join)
            except Exception as error:
                flash(str(error))
                return redirect(url_for('compare_two_tables'))
        db_name_1 = table_meta_data_for_join_1.engine.url.database
        db_name_2 = table_meta_data_for_join_2.engine.url.database
        dialects = [dialect_1, dialect_2]
        table_name_1 = table_meta_data_for_join_1.table_name
        table_name_2 = table_meta_data_for_join_2.table_name
        
        return render_template('joined-preview.html', db_name_1 = db_name_1, db_name_2 = db_name_2, table_name_1 = table_name_1, table_name_2 = table_name_2, db_dialects = dialects, join_attribute_1 = join_attributes[0], join_attribute_2 = join_attributes[1], table_columns = column_names, data = data, unmatched_rows = unmatched_rows, mode = 'compare')


@app.route('/merge', methods = ['GET', 'POST'])
def merge_tables():
    global meta_data_table_1
    global meta_data_table_2
    global compatibility_by_code
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    if tables_in_use != 3:
        if tables_in_use == 0:
            engine_no = 1
        elif tables_in_use == 1:
            engine_no = 2
        return redirect(url_for('show_db_login_page', engine_no = engine_no))
    if request.method == 'GET':
        if compatibility_by_code is None:
            compatibility_by_code = check_basic_data_type_compatibility(meta_data_table_1, meta_data_table_2)
        return show_both_tables_separately(meta_data_table_1, meta_data_table_2, compatibility_by_code, 'merge')
    elif request.method == 'POST':
        global merge_query
        global source_attribute
        global target_attribute
        global query_parameters
        if 'abort-merge' in request.form.keys():
            merge_query = None
            source_attribute = None
            target_attribute = None
            query_parameters = None
            flash('Der Vorgang wurde abgebrochen, daher wurden keine Tabellen verändert.')
            return redirect(url_for('merge_tables'))
        elif 'merge' in request.form.keys():
            target_table_meta_data = None
            source_table_meta_data = None
            if int(request.form['target-table-meta-data']) == 1:
                target_table_meta_data = meta_data_table_1
                source_table_meta_data = meta_data_table_2
            elif int(request.form['target-table-meta-data']) == 2:
                target_table_meta_data = meta_data_table_2
                source_table_meta_data = meta_data_table_1
            try:
                message = execute_merge_and_add_constraints(target_table_meta_data, source_table_meta_data, target_attribute, source_attribute, merge_query, query_parameters)
            except Exception as error:
                source_attribute = None
                target_attribute = None
                merge_query = None
                query_parameters = None
                flash(str(error))
                return redirect(url_for('merge_tables'))
            if target_table_meta_data == meta_data_table_1:
                meta_data_table_1 = update_TableMetaData_entries(target_table_meta_data.engine, target_table_meta_data.table_name)
            elif target_table_meta_data == meta_data_table_2:
                meta_data_table_2 = update_TableMetaData_entries(target_table_meta_data.engine, target_table_meta_data.table_name) 
            compatibility_by_code = check_basic_data_type_compatibility(meta_data_table_1, meta_data_table_2)
            flash(message) 
            source_attribute = None
            target_attribute = None
            merge_query = None
            query_parameters = None
            return redirect(url_for('merge_tables'))
            


@app.route('/merge-preview', methods = ['GET', 'POST'])
def show_merge_preview():
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    elif request.method == 'GET':
        return redirect(url_for('compare'))
    elif request.method == 'POST':
        global merge_query
        global source_attribute
        global target_attribute
        global query_parameters
        target_table = request.form['target-table']
        join_attribute_string = request.form['attribute-selection']
        join_attributes = join_attribute_string.split(', ')
        source_attribute = request.form['source-column-to-insert']
        cast_direction = int(request.form.get('cast'))
        if target_table == 'table_1':
            target_table_data = meta_data_table_1
            target_meta_data_no = 1
            source_table_data = meta_data_table_2
            attributes_to_join_on = join_attributes
        else: 
            target_table_data = meta_data_table_2
            target_meta_data_no = 2
            source_table_data = meta_data_table_1
            attributes_to_join_on = [join_attributes[1], join_attributes[0]]
            if cast_direction == 1:
                cast_direction = 2
            elif cast_direction == 2:
                cast_direction = 1
        if 'target-column' in request.form.keys() and request.form['target-column'] != '':
            target_column = request.form['target-column']
            target_attribute = target_column
        else:      
            target_column = None  
            target_attribute = source_attribute     
        if 'new-attribute-name' in request.form.keys():
            new_column_name = request.form['new-attribute-name']
            for keyword in ('select', 'alter table', 'drop database', 'drop table', 'drop column', 'delete'):
                if keyword in new_column_name.lower():
                    flash('Bitte wählen Sie einen Namen für das neue Attribut, der keine SQL-Schlüsselwörter enthält.')
                    return redirect(url_for('merge_tables'))
        else:
            new_column_name = None
        try:
            merge_result = simulate_merge_and_build_query(target_table_data, source_table_data, attributes_to_join_on, source_attribute, target_column, cast_direction, new_column_name, add_table_names_to_column_names = False)
        except Exception as error:
            flash(str(error))
            return redirect(url_for('merge_tables'))
        else:
            if merge_result is not None and len(merge_result) == 3:
                result = merge_result[0]
                merge_query = merge_result[1]
                query_parameters = merge_result[2]
                if result is None or merge_query is None:
                    flash('Die Datenbankabfrage für die Vorschau konnte nicht erstellt werden. Bitte versuchen Sie es erneut.')
                    return redirect(url_for('merge_tables'))
                if new_column_name is None or new_column_name == '':
                    new_column_name = source_attribute
                db_name_1 = meta_data_table_1.engine.url.database
                db_name_2 = meta_data_table_2.engine.url.database
                db_dialects = [meta_data_table_1.engine.dialect.name, meta_data_table_2.engine.dialect.name]
                table_name_1 = meta_data_table_1.table_name
                table_name_2 = meta_data_table_2.table_name
                table_columns = list(result.keys())
                data = convert_result_to_list_of_lists(result)
                return render_template('joined-preview.html', db_name_1 = db_name_1, db_name_2 = db_name_2, table_name_1 = table_name_1, table_name_2 = table_name_2, db_dialects = db_dialects, db_name = target_table_data.engine.url.database, table_name = target_table_data.table_name, new_column_name = new_column_name, table_columns = table_columns, data = data,  mode = 'merge', target_meta_data_no = target_meta_data_no)
            else:
                flash('Die Datenbankabfrage für die Vorschau konnte nicht erstellt werden. Bitte versuchen Sie es erneut.')
                return redirect(url_for('merge_tables'))



@app.route('/disconnect/<int:engine_no>', methods = ['GET', 'POST'])
def disconnect_from_db(engine_no:int):
    if not session.get('logged_in') or request.method == 'GET':
        return redirect(url_for('start'))
    if request.method == 'POST':
        global engine_1
        global meta_data_table_1
        global engine_2
        global meta_data_table_2
        global db_in_use
        global tables_in_use
        message = ''
        if engine_no == 1:
            message = f'Verbindung zur Datenbank {engine_1.url.database} erfolgreich getrennt.'
            engine_1 = None
            meta_data_table_1 = None
            if tables_in_use == 3:
                meta_data_table_2 = None
            tables_in_use = 0
            db_in_use -= 1
        elif engine_no == 2:
            message = f'Verbindung zur Datenbank {engine_2.url.database} erfolgreich getrennt.'
            engine_2 = None
            meta_data_table_2 = None
            tables_in_use -= 2
            db_in_use -= 2
        elif engine_no == 3:
            message = f'Verbindung zu den Datenbanken {engine_1.url.database} und {engine_2.url.database} erfolgreich getrennt.'
            engine_1 = None
            engine_2 = None
            meta_data_table_1 = None
            meta_data_table_2 = None
            tables_in_use = 0
            db_in_use = 0
            engine_no = 1
        flash(message)
        return redirect(url_for('show_db_login_page', engine_no = engine_no))

@app.route('/logout', methods = ['GET', 'POST'])
def logout():
    if not session.get('logged_in') or request.method == 'GET':
        return redirect(url_for('start'))
    elif request.method == 'POST':
        # Daten der Session entfernen, um den Nutzer auszuloggen
        session.pop('loggedin', None)
        session.pop('username', None)
        # Engines und Tabellenmetadaten zurücksetzen
        global engine_1
        global engine_2
        global db_in_use
        global tables_in_use
        global meta_data_table_1
        global meta_data_table_2
        engine_1 = None
        engine_2 = None
        db_in_use = 0
        tables_in_use = 0
        meta_data_table_1 = None
        meta_data_table_2 = None
        # Weiterleitung zur Login-Seite
        return render_template('login.html', 
                            message = 'Sie wurden erfolgreich abgemeldet. \nBitte loggen Sie sich wieder ein, um das Tool zu nutzen.')


    


if __name__ == '__main__':
    app.secret_key = os.urandom(12)
    serve(app, host = '0.0.0.0', port = 8000)
    