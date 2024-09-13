from flask import Flask, render_template, request, flash, redirect, session, url_for
import os
import re
from sqlalchemy import create_engine
from waitress import serve
from ControllerClasses import TableMetaData
from controllerFunctions import check_validity_of_input_and_searched_value, show_both_tables_separately, update_TableMetaData_entries
from model.SQLDatabaseError import DatabaseError, DialectError
from model.loginModel import register_new_user, login_user 
from model.databaseModel import get_data_type_meta_data, connect_to_db, convert_result_to_list_of_lists, get_primary_key_from_engine, get_row_count_from_engine, list_all_tables_in_db_with_preview, get_full_table_ordered_by_primary_key
from model.oneTableModel import get_indexes_of_affected_attributes_for_replacing, get_replacement_information, get_row_number_of_affected_entries, get_unique_values_for_attribute, replace_all_string_occurrences, replace_some_string_occurrences, search_string, update_to_unify_entries
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

### Routen für das Setup der App ###

# Index-Route
@app.route('/')
def start(): 
    flash('Bitte loggen Sie sich ein, um das Tool zu nutzen.')
    return redirect(url_for('check_login'))
    
# Route für die Anmeldung in der Web-App
@app.route('/login', methods=['POST', 'GET'])
def check_login():
    # Wenn bereits ein Nutzer angemeldet ist, erfolgt die Weiterleitung zur Anmeldeseite für die Datenbanken.
    if session.get('logged_in'):
        return redirect(url_for('show_db_login_page', engine_no = 1))
    # Wurden Anmeldedaten in das Login-Formular eingegeben, ...
    if request.method == 'POST':
        # ... werden diese in login_user überprüft.
        result = login_user(request.form['username'], request.form['password'])
        # Wenn der erste Wert des ausgegebenen Tupels True ist, war die Anmeldung erfolgreich.
        if result[0] == True:
            # Setzen der entsprechenden Session-Variablen
            session['logged_in'] = True
            session['username'] = request.form['username']
            # Weiterleitung zur Anmeldeseite für die Datenbanken
            return redirect(url_for('show_db_login_page', engine_no = 1))
        # Ist der Wert hingegen False, wird die an zweiter Stelle ausgegebene Fehlermeldung auf der als Nächstes angezeigten Seite dargestellt.
        else:
            flash(result[1])
    # Aufruf der Login-Seite
    return render_template('login.html')

# Route für die Registrierung eines neuen Benutzers
@app.route('/register', methods = ['POST', 'GET'])
def register():
    # Bei einem POST-Request wird versucht, anhand der eingegebenen Daten einen neuen Nutzer zu registrieren.
    if request.method == 'POST':
        # Die entsprechende (Fehler-)Meldung wird anschließend im Tool angezeigt.
        message = register_new_user(request.form['username'], request.form['password'])
        flash(message)
    # Sowohl bei GET- als auch bei POST-Requests wird anschließend die Registrierungsseite aufgerufen.
    return render_template('register.html')


### Routen für die Anmeldung an der Datenbank und die Tabellenauswahl ###

# Route für die Anmeldung an einer Datenbank. Der Parameter engine_no dient der Identifizierung, welches Anmeldeformular deaktiviert werden soll
@app.route('/connect-to-db<int:engine_no>', methods = ['GET', 'POST'])
def show_db_login_page(engine_no):
    # Wenn kein Nutzer eingeloggt ist, Weiterleitung zum Login-Formular
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    elif request.method == 'GET':
        # Wenn schon eine Anmeldung an einer Datenbank erfolgt ist, ...
        if engine_no == 2:
            # ... wird deren SQL-Dialekt 'aufgehübscht'
            if engine_1.dialect.name == 'mariadb':
                sql_dialect = 'MariaDB'
            elif engine_1.dialect.name == 'postgresql':
                sql_dialect = 'PostgreSQL'
            # ... und eine Benachrichtigung darüber ausgegeben, welche Tabelle bei der vorangegangenen Meldung ausgewählt wurde.
            flash(f'Tabelle {meta_data_table_1.table_name} der Datenbank {engine_1.url.database} ({sql_dialect}) ausgewählt.')
        # Anzeige des Datenbankformulars. engine_1 wird benötigt, um bei der Anmeldung an der zweiten Datenbank die zuvor eingegebenen Daten für
        # die erste Anmeldung (Benutzername, Server, Portnummer etc.) im linken Anmeldeformular anzeigen zu können.
        return render_template('db-connect.html', user_name = session['username'], engine_no = engine_no, engine_1 = engine_1)
    
# Route für die Datenbankanmeldung, Ziel der von den Anmeldeformularen versendeten POST-Requests 
@app.route('/connect-to-db', methods = ['GET', 'POST'])
def set_up_db_connection():
    # Ohne Anmeldung oder bei Aufruf über GET Umleitung zur Startseite
    if not session.get('logged_in') or request.method == 'GET':
        return redirect(url_for('start'))
    # Bei einem POST-Request wird überprüft, über welchen Button (d. h. aus welchem Formular) die Daten übermittelt wurden.
    elif request.method == 'POST':
        # Wurde der Button 'db-one' betätigt, werden die Daten aus den Eingabefeldern des linken Formulars ausgelesen.
        if 'db-one' in request.form.keys():
            db_name = request.form['db-name1']
            db_dialect = request.form['db-dialect1']
            db_user_name = request.form['user-name1']
            password = request.form['password1']
            host = request.form['host-name1']
            port = request.form['port-number1']
            db_encoding = request.form['encoding1']
        # Für den Button 'db-two' entsprechend die Daten aus den Eingabefeldern des rechten Formulars.
        elif 'db-two' in request.form.keys():
            db_name = request.form['db-name2']
            db_dialect = request.form['db-dialect2']
            db_user_name = request.form['user-name2']
            password = request.form['password2']
            host = request.form['host-name2']
            port = request.form['port-number2']
            db_encoding = request.form['encoding2']
        # Anschließend wird die (versteckt übermittelte) Ziffer der zu belegenden bezogen, hieraus in folgenden Schritten ableiten zu können, 
        # welche Seite angezeigt werden soll.
        engine_no = int(request.form['engine-no'])

        ### Aufbau der Datenbankverbindung ###
        global engine_1
        global engine_2
        global db_in_use
        # Wenn beide Engines nicht None sind, bestehen schon zwei Datenbankverbindungen.
        if engine_1 and engine_2:
            # Daher wird eine entsprechende Meldung ausgegeben, bevor ...
            flash('Sie haben sich bereits mit zwei Datenbanken verbunden.')
            # ... eine Weiterleitung zur "Startseite" der Operationen für zwei Tabellen erfolgt.
            return redirect(url_for('compare_two_tables'))
        else:
            # Anderenfalls wird der Name des aktuell in der Web-App angemeldeten Benutzers bezogen.
            user_name = session['username']
            try: 
                # Erstellung der Engine und Testen der Gültigkeit der Anmeldedaten
                db_engine = connect_to_db(db_user_name, password, host, port, db_name, db_dialect, db_encoding)
            # Treten hierbei Fehler auf, werden sie unter dem neu geladenen Datenbankanmeldeformular angezeigt.
            except (DatabaseError, DialectError) as error:
                flash(str(error))
                return render_template('db-connect.html', user_name = user_name, engine_1 = engine_1, engine_no = engine_no)
            else:
                # Wurde das linke Formular verwendet, 
                if 'db-one' in request.form.keys():
                    engine_no = 1
                    if engine_1 is not None and engine_2 is not None:
                        return render_template('db-connect.html', user_name = user_name, engine_1 = engine_1, engine_no = 2)
                    else:
                        engine_1 = db_engine
                        db_in_use += 1
                elif 'db-two' in request.form.keys():
                    engine_no = 2
                    if engine_1 is None:
                        return render_template('db-connect.html', user_name = user_name, engine_1 = engine_1, engine_no = 1)
                    elif engine_1 is not None and engine_1.url == db_engine.url:
                        engine_2 = None
                        flash('Sie haben sich bereits eine Tabelle aus dieser Datenbank ausgesucht. Bitte verbinden Sie sich mit einer anderen.')
                        return render_template('db-connect.html', user_name = user_name, engine_1 = engine_1, engine_no = engine_no)
                    else:
                        engine_2 = db_engine
                        db_in_use += 2
                return redirect(url_for('select_tables_for_engine', engine_no = engine_no))

# Route für die Auswahl der Tabellen für die Verwendung in der App. Der Parameter engine_no (1 oder 2) wird zur Identifizierung der für die Abfrage
# der existierenden Tabellen zu verwendenden Engine benötigt.            
@app.route('/tables/<int:engine_no>', methods = ['GET'])
def select_tables_for_engine(engine_no:int):
    # Ohne Login Weiterleitung zur Startseite
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    # Aufruf nur per GET-Request
    elif request.method == 'GET':
        # Beträgt der Parameter engine_no 1, wird engine_1 für die Abfrage benötigt.
        if engine_no == 1:
            engine = engine_1
        # Bei einem Wert von 2 entsprechend engine_2.
        elif engine_no == 2:
            engine = engine_2
        # Beziehen der Tabellennamen und ihrer Attribute, der Tabellenvorschau und einer Liste der Tabellen ohne Primärschlüssel (Letztere können
        # in der App nicht sinnvoll verwendet werden).
        tables, previews, tables_without_keys = list_all_tables_in_db_with_preview(engine)
        db_name = engine.url.database
        # Anzeige der Erfolgsmeldung für den Datenbankaufbau.
        flash(f'Verbindung zur Datenbank {db_name} aufgebaut.')
        # Aufruf der Seite für die Tabellenauswahl
        return render_template('tables.html', engine_no = engine_no, db_name = db_name, tables = tables, previews = previews, tables_in_use = tables_in_use, tables_without_keys = tables_without_keys)

# Route für die Festlegung der ausgewählten Tabellen, nur per POST-Request aufrufbar       
@app.route('/tables', methods=['POST', 'GET'])
def select_tables():
    # Ohne Login oder bei nicht bestehender Datenbankverbindung Weiterleitung zur Startseite
    if not session.get('logged_in') or (engine_1 == None and engine_2 == None) or request.method == 'GET':
        return redirect(url_for('start'))
    ### Bereitstellung der globalen Variablen, deren Werte ggf. verändert werden ###
    global tables_in_use
    global meta_data_table_1
    global meta_data_table_2
    # App-Benutzername
    user_name = session['username']
    # Ziffer der zu verwendenden Engine
    engine_no = int(request.form['engine-no'])
    engine = None
    # Namen der Tabellen, die im Select-Element ausgewählt wurden (eines bis zwei für die erste Datenbank, max. eines für die zweite Datenbank)
    table_names = request.form.getlist('selected-table')
    ### Bestimmung der zu verwendenden Engine ###
    if engine_no == 1:
        engine = engine_1
    elif engine_no == 2:
        engine = engine_2
    # Für alle ausgewählten Tabellen werden eine Liste der Primärschlüsselattribute, die Datentypinformationen (Datentyp(gruppe), max. Zeichenanzahl etc.)
    # und die Gesamttupelanzahl bezogen.
    for table_name in table_names:
        primary_keys = get_primary_key_from_engine(engine, table_name)
        data_type_info = get_data_type_meta_data(engine, table_name)
        total_row_count = get_row_count_from_engine(engine, table_name)
        # Anschließend wird das entsprechende TableMetaData-Objekt für den erleichterten Zugriff auf die Tabelle angelegt.
        # Bei tables_in_use = 0 liegt noch kein TableMetaData-Objekt vor.
        if tables_in_use == 0:
            # Daher wird der Zähler um 1 erhöht ...
            tables_in_use += 1
            # ... und das TableMetaData-Objekt für die erste Tabelle mit den zuvor bezogenen Metadaten angelegt.
            meta_data_table_1 = TableMetaData(engine, table_name, primary_keys, data_type_info, total_row_count)
            # Aus diesem werden die Attributnamen bezogen, die für die Anzeige der Seite mit der Suchfunktion benötigt werden, falls nur eine Tabelle
            # ausgewählt wurde.
            columns = meta_data_table_1.columns
        # Steht der Zähler auf 1, wird das zweite TableMetaData-Objekt analog angelegt.
        elif tables_in_use == 1:
            # Der Unterschied besteht darin, dass der Zähler um 2 erhöht wird.
            tables_in_use += 2
            meta_data_table_2 = TableMetaData(engine, table_name, primary_keys, data_type_info, total_row_count)
            columns = meta_data_table_2.columns
        # Steht der Zähler auf einem anderen Wert als 0 oder 1 (d. h. 3), wurden schon zwei Tabellen ausgewählt.
        else:
            # Ausgabe einer entsprechenden Meldung
            flash('Sie haben bereits zwei Tabellen ausgewählt.')
            # Weiterleitung zur Startseite der Vergleichsfunktion
            return redirect(url_for('compare_two_tables'))

    # Wenn der Tabellenzähler nach dem Anlegen aller TableMetaData-Objekte auf 1 steht, wurde nur eine Tabelle der ersten Datenbank ausgewählt.    
    if tables_in_use == 1:
        # In diesem Fall kann unter dem Select-Element eine Checkbox angewählt werden, um eine Verbindung zu einer zweiten Datenbank aufzubauen.
        # Wurde sie angewählt, ist ihr Name 'second-db-checkbox' als Schlüssel im Request-Objekt enthalten.
        if 'second-db-checkbox' in request.form.keys():
            # Ausgabe einer Meldung über den erfolgreichen Verbindungsaufbau zur ersten Datenbank
            flash(f'Tabelle {meta_data_table_1.table_name} aus der Datenbank {engine.url.database} ausgewählt.')
            # Aufruf des Datenbankanmeldeformulars für die zweite Datenbank
            return redirect(url_for('show_db_login_page', engine_no = 2))
        # Wurde die Checkbox nicht angewählt, soll nur mit einer Tabelle gearbeitet werden.
        else:
            # Daher werden der Datenbankname und alle Einträge dieser Tabelle bezogen ...
            db_name = meta_data_table_1.engine.url.database
            data = get_full_table_ordered_by_primary_key(meta_data_table_1)
            # ... und auf der Seite der Suchfunktion angezeigt.
            return render_template('search.html', user_name = user_name, db_name = db_name, table_name = table_name, table_columns = columns, data = data, searched_string = '')  
    # Wenn der Tabellenzähler auf 3 steht, wurden zwei Tabellen ausgewählt.
    elif tables_in_use == 3:
        # Daher erfolgt eine Weiterleitung zur Startseite der Vergleichsfunktion.
        return redirect(url_for('compare_two_tables'))
        

##### Routen für die Operationen auf einer Tabelle #####
 
# Route für die Suche von Strings in Tabellen
@app.route('/search', methods = ['POST', 'GET'] )
def search_entries():
    # Ohne Login Weiterleitung zur Startseite
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    # Bei Auswahl von zwei Tabellen Weiterleitung zur Vergleichsfunktion als Startseite für die Operationen auf zwei Tabellen
    if tables_in_use == 3:
        return redirect(url_for('compare_two_tables'))
    # Wenn noch keine Tabelle ausgewählt wurde, Weiterleitung zur Datenbankanmeldung
    elif tables_in_use == 0:
        return redirect(url_for('set_up_db_connection'))
    # Anwendungsbenutzername
    user_name = session['username']
    ### für die Suche benötigte Metadaten der ausgewählten Tabelle ###
    db_name = engine_1.url.database
    table_name = meta_data_table_1.table_name
    table_columns = meta_data_table_1.columns
    # Anlegen des Suchstrings
    searched_string = ''
    # Beziehen aller Einträge der aktuellen Tabelle
    full_table = get_full_table_ordered_by_primary_key(meta_data_table_1)
    data = []
    # Bei einem Aufruf per GET-Request werden alle Einträge der aktuellen Tabelle angezeigt.
    if request.method == 'GET':
        data = full_table
    # Bei einem Aufruf per POST-Request ...
    elif request.method == 'POST':
        # ... wird der Name des zu durchsuchenden Attributs aus dem Request bezogen.
        column_name = request.form['column-to-search']
        # Lautet dieser 'all', sollen alle Spalten nach dem eingegebenen String durchsucht werden.
        if column_name == 'all':
            # Daher entsprechen die bei der SQL-Abfrage berücksichtigten Attribute der Attributliste des TableMetaData-Objekts der aktuellen Tabelle.
            column_names = meta_data_table_1.columns
        else:
            # Anderenfalls wird der aus dem Request bezogene Attributname in eine Liste mit nur einem Eintrag erwartet, da search_string eine Liste 
            # als Parameter erfordert.
            column_names = [column_name]
        # Beziehen des zu suchenden Strings aus dem Request
        string_to_search = request.form['search-string']
        # Ist dieser leer, wird die volle Tabelle angezeigt.
        if string_to_search == '':
            data = full_table
        else:
            # Anderenfalls wird die Suche nach dem String per SQL-Abfrage in der Datenbank ausgeführt.
            data = search_string(meta_data_table_1, string_to_search, column_names)
    # Anzeige der Seite mit der Suchfunktion
    return render_template('search.html', user_name = user_name, db_name = db_name, table_name = table_name, searched_string = string_to_search, table_columns = table_columns, data = data)

### Routen für die Funktion 'Suchen und Ersetzen' ###

# Route für die Start- und Zielseite der Funktion 'Suchen und Ersetzen'
@app.route('/replace', methods = ['GET', 'POST'])
def search_and_replace_entries():
    # Ohne Login Weiterleitung zur Startseite
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    # Anwendungsbenutzername
    user_name = session['username']
    ### Beziehen der für die Bearbeitung von GET- und POST-Requests nötigen Metadaten der Tabelle ###
    db_name = engine_1.url.database
    table_name = meta_data_table_1.table_name
    table_columns = meta_data_table_1.columns
    # Bei einem GET-Request wird die volle Tabelle nach Primärschlüsseln geordnet aus der Datenbank bezogen und angezeigt.
    if request.method == 'GET':
        data = get_full_table_ordered_by_primary_key(meta_data_table_1)
        return render_template('replace.html', user_name = user_name, db_name = db_name, table_name = table_name, table_columns = table_columns, data = data)
    # Bei einem POST-Request wird der zuvor über die Vorschau bestätigte Ersetzungsvorgang ausgeführt.
    elif request.method == 'POST':
        # Bereitstellung des globalen Dictionarys zur Identifizierung der zu ersetzenden Vorkommen
        global replacement_occurrence_dict
        # zu ersetzender String
        string_to_replace = request.form['string-to-replace']
        # neu in die Datenbank einzutragender Wert
        replacement_string = request.form['replacement-string']
        # Die Liste der Spaltennamen aus der Vorschau wird im Request als String übermittelt. Daher müssen hier alle Zeichen entfernt werden, die
        # die Liste in Python kennzeichnen (eckige Klammern und Anführungszeichen um die Namen herum)
        column_names = request.form['affected-attributes'].removeprefix('[').removesuffix(']').replace('\'', '').split(', ')
        # Beziehen der Nummern der Vorkommen des Suchstrings, die verändert werden sollen
        affected_occurrences = request.form.getlist('selection')
        # Anzahl aller Vorkommen zur Anzeige in der Statistik unter der Tabelle
        total_occurrences = len(replacement_occurrence_dict)
        # Erstellen einer Kopie des Dictionarys mit den Vorkommen des gesuchten Wertes, damit die nicht ausgewählten Werte hieraus entfernt werden können
        occurrences_to_change = replacement_occurrence_dict.copy()
        ### Entfernen aller Elemente aus dem Dictionary, deren Schlüssel nicht in der mit dem Request übermittelten Auswahl enthalten sind ###
        for key in replacement_occurrence_dict.keys():
            if str(key) not in affected_occurrences:
                occurrences_to_change.pop(key)
        # Wenn die Anzahl der ausgewählten Vorkommen der Anzahl aller Vorkommen des Strings entspricht, ...
        if len(occurrences_to_change.keys()) == total_occurrences:
            try:
                # ... sollen alle Vorkommen ersetzt werden.
                data = replace_all_string_occurrences(meta_data_table_1, column_names, string_to_replace, replacement_string, commit=True)
            # Die Meldungen zu hierbei auftretenden Fehlern werden gespeichert.
            except Exception as error:
                message = str(error)
            # Funktioniert der Vorgang, wird eine Erfolgsmeldung erstellt.
            else:
                message = f'Alle {total_occurrences} Ersetzungen wurden erfolgreich vorgenommen.'
        # Wenn keine Vorkommen ausgewählt wurden, wird wieder die volle Tabelle angezeigt, mit einem Hinweis, dass keine Ersetzungen vorgenommen wurden.
        elif len(occurrences_to_change.keys()) == 0:
            data = get_full_table_ordered_by_primary_key(meta_data_table_1)
            message = 'Es wurden keine Einträge ausgewählt, daher wurde nichts verändert.'
        # Anderenfalls sollen nur einige ausgewählte Vorkommen des Suchstrings ersetzt werden.
        else:
            # Einfügen der Primärschlüsselattribute in das Dictionary der einzufügenden Werte, damit diese an die Funktion replace_some_string_occurrences
            # übergeben werden können. Der Zähler für die Vorkommen des gesuchten Wertes beginnt bei 1.
            occurrences_to_change[0] = {'primary_keys': meta_data_table_1.primary_keys}
            # Ausführen der Ersetzung und Übernahme der dabei ausgegebenen Meldung
            message = replace_some_string_occurrences(meta_data_table_1, occurrences_to_change, string_to_replace, replacement_string, commit=True)
            # Anschließend wird die volle aktualisiert Tabelle bezogen.
            data = get_full_table_ordered_by_primary_key(meta_data_table_1)
        ### Zurücksetzen des Dictionarys mit den zu ersetzenden Vorkommen ###
        replacement_occurrence_dict = None
        # Ausgabe der Meldung
        flash(message)
        # Anzeige der Startseite der Ersetzungsfunktion
        return render_template('replace.html', user_name = user_name, db_name = db_name, table_name = table_name, table_columns = table_columns, data = data)

# Route für die Anzeige der Vorschau für das Ersetzen von (Teil-)Strings
@app.route('/replace-preview', methods = ['GET', 'POST'])
def select_entries_to_update():
    # Ohne Login Weiterleitung zur Startseite
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    ### Beziehen der für die Bearbeitung von GET- und POST-Requests benötigten Variablen ###
    user_name = session['username']
    db_name = engine_1.url.database
    table_name = meta_data_table_1.table_name
    table_columns = meta_data_table_1.columns
    # /replace-preview kann nicht über GET-Requests aufgerufen werden
    if request.method == 'GET':
        data = get_full_table_ordered_by_primary_key(meta_data_table_1, convert = False)
        return render_template('replace.html', user_name = user_name, db_name = db_name, table_name = table_name, table_columns = table_columns, data = data)
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
       
        message = ''
        # Nun wird die Attributliste mit entweder nur Einsen oder nur Nullen und einer Eins durchiteriert ...       
        for index, attribute in enumerate(attribute_list):
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
            return render_template('replace.html', user_name = user_name, db_name = db_name, table_name = table_name, table_columns = table_columns, data = unchanged_data)
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
            flash(str(error))
            # ... und der Nutzer zur Startseite der Suchen-und-Ersetzen-Funktion weitergeleitet.
            return render_template('replace.html', user_name = user_name, db_name = db_name, table_name = table_name, table_columns = table_columns, data = unchanged_data)
        # Falls keine Fehler auftreten, ...
        else:
            # ... das Ergebnis jedoch leer ist, ...
            if len(replacement_data_dict.values()) == 0:
                # ... wird dies auf der Webseite angezeigt.
                flash('Keine passenden Einträge gefunden.')
                return render_template('replace.html', user_name = user_name, db_name = db_name, table_name = table_name, table_columns = table_columns, data = unchanged_data)
            else:
                # Anderenfalls werden die Meldungen für den Ausschluss von Attributen angezeigt ...
                flash(message)
                # ... und mithilfe der gewonnenen Daten wird unter localhost:8000/replace-preview die Vorschau erstellt.
                return render_template('replace-preview.html', user_name = user_name, db_name = db_name, table_name = table_name, occurrence_dict = replacement_occurrence_dict, table_columns = table_columns, string_to_replace = string_to_search, replacement_string = input, replacement_data_dict = replacement_data_dict, affected_attributes = affected_attributes)


### Routen für das Vereinheitlichen von Datenbankeinträgen ###

@app.route('/unify', methods = ['GET', 'POST'])
def unify_db_entries():
    # Ohne Login Weiterleitung zur Startseite
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    # Beziehen des Anwendungsbenutzernamens
    user_name = session['username']
    if request.method == 'GET':
        data = get_full_table_ordered_by_primary_key(meta_data_table_1)
    elif request.method == 'POST':
        ### Beziehen der für die Anzeige der Startseite der Vereinheitlichungsfunktion benötigten Tabellenmetadaten ###
        db_name = engine_1.url.database
        table_name = meta_data_table_1.table_name
        table_columns = meta_data_table_1.columns
        primary_keys = meta_data_table_1.primary_keys
        # Anlegen der Nachricht, die auf der als Nächsts geladenen Seite der App ausgegeben wird
        message = ''
        # Beziehen des für die Vereinheitlichung ausgewählten Attributs aus dem Request
        attribute_to_change = request.form['column-to-unify']
        # Beziehen der zu vereinheitlichenden Werte aus dem Request
        # \ wird beim Auslesen zu \\. Dies muss rückgängig gemacht werden, weil diese Parameter zweimal an HTML-Dokumente gesendet werden
        old_values = request.form['old-values'].replace('[', '').replace(']', '').replace('\'', '').replace('\\\\', '\\').split(', ')
        # Beziehen des einzusetzenden, vereinheitlichten Wertes aus dem Request
        new_value = request.form['new-value']
        # Durchführung der Datenbankaktualisierung
        try:
            update_to_unify_entries(meta_data_table_1, attribute_to_change, old_values, new_value, True)
        # Hierbei auftretende Fehler werden als Meldung übernommen.
        except Exception as error:
            message = str(error)
        # Anderenfalls wird eine Erfolgsmeldung festgelegt.
        else:  
            message = 'Änderungen erfolgreich durchgeführt.'
        # Beziehen der vollen, nach Primärschlüsseln geordneten Tabelle für die Anzeige
        data = get_full_table_ordered_by_primary_key(meta_data_table_1)
        # Ausgabe der Fehler- oder Erfolgsmeldung
        flash(message)
    # Anzeige der Startseite der Vereinheitlichungsfunktion    
    return render_template('unify.html', user_name = user_name, db_name = db_name, table_columns = table_columns, primary_keys = primary_keys, data = data, table_name = table_name, engine_no = 1)    

# Route für die Anzeige der Seite zur Auswahl der zu vereinheitlichenden Einträge
@app.route('/unify-selection', methods = ['GET', 'POST'])
def select_entries_to_unify():
    # Ohne Login Weiterleitung zur Startseite
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    # Bei Aufruf per GET-Request Weiterleitung zur Ausgangsseite der Vereinheitlichungsfunktion
    elif request.method == 'GET':
        return redirect(url_for('unify_db_entries'), code = 302)
    # Gültiger Aufruf nur per POST-Request
    elif request.method == 'POST':
        # Beziehen des Benutzernamens
        user_name = session['username']
        ### Beziehen des Datenbanknamens, des Tabellennamens und des von der Vereinheitlichung betroffenen Attributs für die Anzeige ###
        db_name = engine_1.url.database
        table_name = request.form['table-name']
        column_to_unify = request.form['column-to-unify']
        # Beziehen der einzigartigen Werte in der Tabelle, inkl. der Anzahl ihrer Vorkommen
        data = get_unique_values_for_attribute(meta_data_table_1, column_to_unify)
        # Anzeige in unify-selection.html
        return render_template('unify-selection.html', user_name = user_name, db_name = db_name, table_name = table_name, column_to_unify = column_to_unify, data = data, engine_no = 1)

# Route für die Anzeige der Vereinheitlichungsvorschau 
@app.route('/unify-preview', methods = ['GET', 'POST'])
def show_affected_entries():
    # Ohne Login Weiterleitung zur Startseite
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    # Bei Aufruf über GET wird die Ausgangsseite der Vereinheitlichungsfunktion aufgerufen
    elif request.method == 'GET':
        return redirect(url_for('unify_db_entries'), code = 302)
    # Gültiger Aufruf nur per POST-Request
    elif request.method == 'POST':
        # Beziehen des Benutzernamens
        user_name = session['username']
        ### Beziehen der Argumente für update_to_unify_entries aus dem Request ### 
        db_name = engine_1.url.database
        table_name = meta_data_table_1.table_name
        table_columns = meta_data_table_1.columns
        column_to_unify = request.form['column-to-unify']
        new_value = request.form['replacement']
        # einheitlicher Umgang mit leeren Werten
        if new_value in ('', 'None', 'NULL', None):
            new_value = None
        # Beziehen der zu ersetzenden Werte, ...
        old_values = list()
        for key in request.form.keys():
            # ... die in der HTML-Datei über ganzzahlige Schlüssel (Namen) identifiziert sind.
            if re.match(r'^[0-9]+$', key):
                old_values.append(request.form[key])
        # Beziehen der einzigartigen Einträge im ausgewählten Attribut der Tabelle, inkl. der Anzahl der Vorkommen
        unique_values = get_unique_values_for_attribute(meta_data_table_1, column_to_unify)
        # Es muss mindestes ein Wert ausgewählt werden, der verändert werden soll. Ist das nicht der Fall, ...
        if len(old_values) < 1:
            data = unique_values
            # ... wird hierzu eine Fehlermeldung ausgegeben ...
            flash('Bitte wählen Sie mindestens ein zu bearbeitendes Attribut aus und versuchen Sie es erneut.')
            # ... und die Auswahl erneut aufgerufen.
            return render_template('unify-selection.html', user_name = user_name, db_name = db_name, table_name = table_name, column_to_unify = column_to_unify, data = data, engine_no = 1)
        # Stimmt die Anzahl der ausgewählten Einträge, wird überprüft, ob der neue Wert mit zu verändernden Attribut kompatibel ist.
        validity = check_validity_of_input_and_searched_value(meta_data_table_1, new_value, column_to_unify, old_values[0])
        # Bei Kompatibilität wird hier der Wert 0 ausgegeben. Ist das nicht der Fall, ...
        if validity != 0:
            # ... wird die erhaltene Fehlermeldung ...
            flash(validity)
            # ... auf der neu geladenen Seite mit der Auswahl der zu vereinheitlichenden Vorkommen angezeigt.
            return render_template('unify-selection.html', user_name = user_name, db_name = db_name, table_name = table_name, column_to_unify = column_to_unify, data = data, engine_no = 1)
        # Besteht Kompatibilität, ...
        else:
            try:
                # ... wird die Änderung 'simuliert', d. h. ausgeführt, aber durch Auslassen von commit() nicht in die Datenbank geschrieben.
                update_to_unify_entries(meta_data_table_1, column_to_unify, old_values, new_value, False)
            # Treten hierbei Fehler auf, ...
            except Exception as error:
                # ... werden diese für Constraints gefiltert ...
                if 'constraint' in str(error).lower():
                    flash('Der eingegebene neue Wert verletzt eine Bedingung (Constraint) Ihrer Datenbank. Bitte versuchen Sie es erneut.')
                # ... und ansonsten unverändert ausgegeben.
                else:
                    flash(str(error))
                data = unique_values
                # Auch in diesem Fall wird die Auswahl für die zu vereinheitlichenden Einträge mit der Fehlermeldung erneut aufgerufen.
                return render_template('unify-selection.html', user_name = user_name, db_name = db_name, table_name = table_name, column_to_unify = column_to_unify, data = data, engine_no = 1)
        
        ### Beziehen der unveränderten und der aktualisierten Daten für die dynamische Darstellung im Browser
        # unveränderte Tabelle
        data = get_full_table_ordered_by_primary_key(meta_data_table_1)
        # Abfrage der Zeilennummer der von der Vereinheitlichung betroffenen Einträge (von 1 an gezählt)
        affected_entries = get_row_number_of_affected_entries(meta_data_table_1, [column_to_unify], old_values, mode = 'unify')
        # Herausfiltern der Nummern aus dem CursorResult in eine Liste
        affected_rows = []
        for row in affected_entries:
            affected_rows.append(row[0])
        # Feststellung der Position des betroffenen Attributs in der Anzeige
        index_of_affected_attribute = 0
        for index, column in enumerate(table_columns):
            # Wenn das aktuelle Attribut das von der Änderung betroffene ist, ...
            if column == column_to_unify:
                # ... entspricht seine Position in der Anzeige dem Index in der Attributliste + 1 (weil der Zähler in der HTML-Datei bei 1 beginnt)
                index_of_affected_attribute = index + 1
                # Da nur ein Attribut gleichzeitig von der Vereinheitlichung betroffen ist, kann die Schleife anschließend abgebrochen werden.
                break
        # Gesamtanzahl der Tupel für die Statistik unter der Tabelle
        row_total = meta_data_table_1.total_row_count
        # Anzeige der Vorschau
        return render_template('unify-preview.html', user_name = user_name, db_name = db_name, table_name = table_name, table_columns = table_columns, column_to_unify = column_to_unify, old_values = old_values, new_value = new_value, data = data, index_of_affected_attribute = index_of_affected_attribute, affected_rows = affected_rows, row_total = row_total)


##### Routen für die Operationen auf zwei Tabellen #####

# Route für die Vergleichsfunktion (Join)
@app.route('/compare', methods = ['GET', 'POST'])
def compare_two_tables():
    # Ohne Login Weiterleitung zur Startseite
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    # Weiterleitungen, falls nicht genau zwei Tabellen ausgewählt sind
    if tables_in_use != 3:
        # Wurde noch keine Tabelle ausgewählt, erfolgt eine Weiterleitung zum Datenbankanmeldeformular
        if tables_in_use == 0:
            return redirect(url_for('show_db_login_page', engine_no = 1))
        # Wurde nur eine Tabelle ausgewählt, erfolgt eine Weiterleitung zur Seite der Suchfunktion
        elif tables_in_use == 1:
            return redirect(url_for('search_entries'))
    # Anwendungsbenutzername
    user_name = session['username']
    ### Ermittlung der (groben) Kompatibilität für die Anzeige der Auswahl der Join-Attribute ###
    global compatibility_by_code
    if compatibility_by_code is None:
        compatibility_by_code = check_basic_data_type_compatibility(meta_data_table_1, meta_data_table_2)
    # Bei einem GET-Request werden beide Tabellen unverändert angezeigt.
    if request.method == 'GET':
        return show_both_tables_separately(meta_data_table_1, meta_data_table_2, compatibility_by_code, 'compare', user_name)
    # Bei einem POST-Request wird das Join-Ergebnis als einzelne Tabelle angezeigt.
    elif request.method == 'POST':
        ### Beziehen der für die Ausführung des Joins benötigten Informationen aus dem Request ###
        # Zieltabelle des Joins (deren Werte in der Ausgabe zuerst angeführt werden); 'table_1' oder 'table_2'
        target_table = request.form['target-table']
        # Ermittlung der Join-Attribute, die der Request als einen String übermittelt, ...
        join_attribute_string = request.form['attribute-selection']
        # ... sodass dieser anhand des Kommas in eine Liste von Attributnamen umgewandelt werden kann.
        join_attributes = join_attribute_string.split(', ')
        # Wert für die Richtung der Datentypkonversion; 0 für keine (erzwungene) Konversion, 1 für erzwungene Konversion des Join-Attributs der
        # Zieltabelle, 2 für erzwungene Konversion des Join-Attributs der zweiten Tabelle
        cast_direction = int(request.form.get('cast'))
        # Flag für die Erstellung eines Full Outer Join; standardmäßig False
        full_outer_join = False
        # Dieses wird nur auf True gesetzt, wenn die Checkbox für den Full Outer Join unter localhost:8000/compare aktiviert wurde.
        if 'full-outer-join' in request.form.keys():
            full_outer_join = True
        # Wenn die als Zweites ausgewählte Tabelle in der Join-Ansicht vorne stehen soll, müssen einige Anpassungen der an die Verbindungsfunktion
        # übergebenen Werte vorgenommen werden.
        if target_table == 'table_2':
            ### "Tausch" der Reihenfolge der TableMetaData-Objekte ###
            table_meta_data_for_join_1 = meta_data_table_2
            table_meta_data_for_join_2 = meta_data_table_1
            # Die ausgewählten Attribute der zweiten Tabelle werden der Funktion an erster Stelle übergeben, ...
            selected_columns_table_1 = request.form.getlist('columns-table2')
            # ... die Auswahl für die erste Tabelle entsprechend an zweiter Stelle.
            selected_columns_table_2 =  request.form.getlist('columns-table1')
            # Auch die Reihenfolge der Join-Attribute in der Liste wird vertauscht.
            attributes_to_join_on = [join_attributes[1], join_attributes[0]]
            # Ebenso muss der Wert für die Typkonversionsrichtung getauscht werden, falls dieser nicht 0 beträgt.
            if cast_direction == 1:
                cast_direction = 2
            elif cast_direction == 2:
                cast_direction = 1
        # Wenn die bei der Anmeldung als Erstes ausgewählte Tabelle als Zieltabelle dienen soll, werden die Daten in der Reihenfolge übernommen,
        # wie sie im Request übermittelt wurden.
        else:
            ### TableMetaData-Objekte für den Join ###
            table_meta_data_for_join_1 = meta_data_table_1
            table_meta_data_for_join_2 = meta_data_table_2
            # ausgewählte Attribute der ersten Tabelle
            selected_columns_table_1 =  request.form.getlist('columns-table1')
            # ausgewählte Attribute der zweiten Tabelle
            selected_columns_table_2 = request.form.getlist('columns-table2')
            # Liste der Join-Attribute
            attributes_to_join_on = join_attributes
        
        # Zusammenfügen der TableMetaData-Objekte zu einer Liste, da die Vergleichsfunktion dies erfordert
        meta_data_list = [table_meta_data_for_join_1, table_meta_data_for_join_2]

        ### Informationen für die Feststellung, ob der SQL- oder der Python-basierte Join erfolgen soll ###  
        # SQL-Dialekte der beiden Tabellen   
        dialect_1 = table_meta_data_for_join_1.engine.dialect.name
        dialect_2 = table_meta_data_for_join_2.engine.dialect.name
        # URLs der beiden Engines
        url_1 = table_meta_data_for_join_1.engine.url
        url_2 = table_meta_data_for_join_2.engine.url
        # Wenn beide URLs übereinstimmen (d. h. die Tabellen in derselben Datenbank liegen) oder beides MariaDB-Tabellen auf demselben Server sind, ...
        if url_1 == url_2 or (dialect_1 == 'mariadb' and dialect_2 == 'mariadb' and url_1.host == url_2.host and url_1.port == url_2.port):
            try:
                # ... wird der SQL-basierte Join ausgeführt.
                data, column_names, unmatched_rows = join_tables_of_same_dialect_on_same_server(meta_data_list, attributes_to_join_on, selected_columns_table_1, selected_columns_table_2, cast_direction, full_outer_join)
            except Exception as error:
                # Hierbei auftretende Fehler werden als Meldung in der App auf der Seite der Vereinheitlichungsfunktion angezeigt.
                flash(str(error))
                return redirect(url_for('compare_two_tables'))
        # Wenn die Tabellen in verschiedenen Datenbanken liegen (PostgreSQL), sich die Server oder die Dialekte der Tabellen unterscheiden, ...
        else:
            try:
                # ... wird der Python-basierte Join ausgeführt.
                data, column_names, unmatched_rows = join_tables_of_different_dialects_dbs_or_servers(meta_data_list, attributes_to_join_on, selected_columns_table_1, selected_columns_table_2, cast_direction, full_outer_join)
            except Exception as error:
                # Auftretende Fehler werden wie zuvor auf der neu geladenen Seite der Vergleichsfunktion angezeigt.
                flash(str(error))
                return redirect(url_for('compare_two_tables'))
        ### War der Join hingegen erfolgreich, werden noch die Datenbanknamen, die SQL-Dialekte und die Tabellennamen für die Anzeige des 
        # Ergebnisses benötigt. ###
        db_name_1 = table_meta_data_for_join_1.engine.url.database
        db_name_2 = table_meta_data_for_join_2.engine.url.database
        dialects = [dialect_1, dialect_2]
        table_name_1 = table_meta_data_for_join_1.table_name
        table_name_2 = table_meta_data_for_join_2.table_name
        # Anzeige des Join-Ergebnisses
        return render_template('joined-preview.html', user_name = user_name, db_name_1 = db_name_1, db_name_2 = db_name_2, table_name_1 = table_name_1, table_name_2 = table_name_2, db_dialects = dialects, join_attribute_1 = join_attributes[0], join_attribute_2 = join_attributes[1], table_columns = column_names, data = data, unmatched_rows = unmatched_rows, mode = 'compare')


### Routen für die Attributübertragung ###


@app.route('/merge', methods = ['GET', 'POST'])
def merge_tables():
    # Ohne Login Weiterleitung zur Startseite
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    # Wenn nicht genau zwei Tabellen ausgewählt sind, kann die Attributübertragungsfunktion nicht ausgeführt werden.
    if tables_in_use != 3:
        # Wenn noch keine Tabelle festgelegt wurde, erfolgt eine Weiterleitung zur Tabellenauswahl.
        if tables_in_use == 0:
            return redirect(url_for('select_tables_for_engine', engine_no = 1))
        # Wenn nur eine Tabelle ausgewählt ist, erfolgt eine Weiterleitung zur Seite der Suchfunktion als Startseite der Operationen auf zwei Tabellen.
        elif tables_in_use == 1:
            return redirect(url_for('search_entries'))
    ### Bereitstellung der ggf. veränderten globalen Variablen ###
    global meta_data_table_1
    global meta_data_table_2
    global merge_query
    global source_attribute
    global target_attribute
    global query_parameters
    global compatibility_by_code
    user_name = session['username']
    # Bei einem GET-Request ... 
    if request.method == 'GET':
        # ... wird zunächst das Kompatibilitäts-Dictionary ermittelt, falls dieses noch nicht existiert, ...
        if compatibility_by_code is None:
            compatibility_by_code = check_basic_data_type_compatibility(meta_data_table_1, meta_data_table_2)
        # und anschließend die Startseite für die Attributsübertragung angezeigt.
        return show_both_tables_separately(meta_data_table_1, meta_data_table_2, compatibility_by_code, 'merge', user_name)
    # Bei einem POST-Request werden die Attributsübertragung und ggf. das Einfügen von Constraints mithilfe der zuvor erstellten Abfragen ausgeführt.
    elif request.method == 'POST':
        # Bei Klick auf den Button 'Abbrechen' in der Vorschau ...
        if 'abort-merge' in request.form.keys():
            # ... werden die zuvor global angelegte Abfrage und ihre Parameter gelöscht ...
            merge_query = None
            source_attribute = None
            target_attribute = None
            query_parameters = None
            # ... und auf der Startseite der Attributübertragung wird über den unbearbeiteten Tabellen eine Meldung hierzu angezeigt.
            flash('Der Vorgang wurde abgebrochen, daher wurden keine Tabellen verändert.')
            return redirect(url_for('merge_tables'))
        ### Durchführung der Attributübertragung bei Bestätigung in der Vorschau ###
        elif 'merge' in request.form.keys():
            target_table_meta_data = None
            source_table_meta_data = None
            ### Die Reihenfolge der Tabellen geht aus dem Wert für 'target-table-meta-data' im Request hervor. ###
            if int(request.form['target-table-meta-data']) == 1:
                target_table_meta_data = meta_data_table_1
                source_table_meta_data = meta_data_table_2
            elif int(request.form['target-table-meta-data']) == 2:
                target_table_meta_data = meta_data_table_2
                source_table_meta_data = meta_data_table_1
            # Ausführung der zuvor erstellten Abfrage(n)
            try:
                message = execute_merge_and_add_constraints(target_table_meta_data, source_table_meta_data, target_attribute, source_attribute, merge_query, query_parameters)
            # Fehler werden wie ein Abbruch gehandhabt.
            except Exception as error:
                ### D. h. die globalen Variablen für die Attributübertragung werden zurückgesetzt ... ###
                source_attribute = None
                target_attribute = None
                merge_query = None
                query_parameters = None
                # ... und die Fehlermeldung wird ...
                flash(str(error))
                # ... auf der Startseite der Attributübertragung angezeigt.
                return redirect(url_for('merge_tables'))
            ### Bei Erfolg muss nun noch das TableMetaData-Objekt der Zieltabelle der Übertragung aktualisiert werden. ###
            if target_table_meta_data == meta_data_table_1:
                meta_data_table_1 = update_TableMetaData_entries(target_table_meta_data.engine, target_table_meta_data.table_name)
            elif target_table_meta_data == meta_data_table_2:
                meta_data_table_2 = update_TableMetaData_entries(target_table_meta_data.engine, target_table_meta_data.table_name) 
            # Auch das Kompatibilitäts-Dictionary wird aktualisiert, da es sich verändert haben könnte.
            compatibility_by_code = check_basic_data_type_compatibility(meta_data_table_1, meta_data_table_2)
            # Zuletzt wird auch bei Erfolg eine Meldung ausgegeben ...
            flash(message) 
            ### ... und die globalen Variablen für die Übertragung werden zurückgesetzt. ###
            source_attribute = None
            target_attribute = None
            merge_query = None
            query_parameters = None
            # Anzeige der Startseite der Attributübertragung mit den vollständigen (im Fall der Zieltabelle: aktualisierten) Tabellen
            return redirect(url_for('merge_tables'))
            

# Route für die Anzeige der Vorschau der Attributsübertragung
@app.route('/merge-preview', methods = ['GET', 'POST'])
def show_merge_preview():
    # Ohne Login Weiterleitung zur Startseite
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    # Bei Aufruf per GET-Request Weiterleitung zur Ausgangsseite für die Attributübertragungsfunktion
    elif request.method == 'GET':
        return redirect(url_for('merge'))
    # Gültiger Aufruf nur per POST-Request
    elif request.method == 'POST':
        ### Bereitstellung der globalen Variablen, die für die Attributübertragung angelegt werden müssen. ###
        global merge_query
        global source_attribute
        global target_attribute
        global query_parameters
        # Zieltabelle der Übertragung ('table_1' oder 'table_2')
        target_table = request.form['target-table']
        # Beziehen der Join-Attribute als String aus dem Request
        join_attribute_string = request.form['attribute-selection']
        # Umwandlung des Strings in eine Liste von Attributnamen
        join_attributes = join_attribute_string.split(', ')
        # Beziehen des Namens des zu übertragenden Attributs aus dem Request
        source_attribute = request.form['source-column-to-insert']
        # Beziehen der Datentypkonversionsrichtung; 0 für keine (erzwungene) Konversion, 1 für Konversion des Join-Attributs der ersten Tabelle,
        # 2 für Konversion des Join-Attributs der zweiten Tabelle
        cast_direction = int(request.form.get('cast'))
        # Wenn die erste Tabelle die Zieltabelle ist, ...
        if target_table == 'table_1':
            # ... wird ihr TableMetaData-Objekt als Ziel-TableMetaData-Objekt übernommen.
            target_table_data = meta_data_table_1
            # Die zweite Tabelle ist entsprechend die Quelltabelle.
            source_table_data = meta_data_table_2
            # Außerdem wird der Wert 1 als verstecktes Input-Element in joined-preview integriert, um die Zieltabelle bei der Ausführung der
            # Übertragung identifizieren zu können.
            target_meta_data_no = 1
            # Die Reihenfolge der Join-Attribute in der aus dem Request ermittelten Liste wird beibehalten.
            attributes_to_join_on = join_attributes
        # Wenn die zweite Tabelle die Zieltabelle ist, sind diese Werte zu vertauschen.
        else: 
            # Die zweite Tabelle ist die Zieltabelle ...
            target_table_data = meta_data_table_2
            # ... und die erste entsprechend die Quelltabelle.
            source_table_data = meta_data_table_1
            # Dies wird in Form des Wertes 2 bei der Ausführung der Übertragung an localhost:8000/merge übermittelt.
            target_meta_data_no = 2
            # Die Reihenfolge der Join-Attribute wird vertauscht.
            attributes_to_join_on = [join_attributes[1], join_attributes[0]]
            ### Außerdem muss die Typkonversionsrichtung auf das jeweils andere Join-Attribut bezogen werden. ###
            if cast_direction == 1:
                cast_direction = 2
            elif cast_direction == 2:
                cast_direction = 1
        # Wenn in der App ein bestehendes Attribut der Zieltabelle als Ziel der Übertragung angegeben wurde, ...
        if 'target-column' in request.form.keys() and request.form['target-column'] != '':
            # ... wird dessen Name aus dem Request bezogen.
            target_column = request.form['target-column']
            target_attribute = target_column
        else:     
            # Anderenfalls wird der Name des Quellattributs als Name des neu einzufügenden Zielattributs übernommen. 
            target_column = None  
            target_attribute = source_attribute 
        # Wenn ein Name für das neue Attribut angegeben wurde, ...    
        if 'new-attribute-name' in request.form.keys():
            # ... wird dieser aus dem Request bezogen.
            new_attribute_name = request.form['new-attribute-name']
            # Falls dieser grundlegende SQL-Schlüsselwörter enthält, wird die Operation abgebrochen.
            for keyword in ('select', 'alter table', 'drop database', 'drop table', 'drop column', 'delete'):
                if keyword in new_attribute_name.lower():
                    # Auf der Startseite der Übertragungsfunktion wird zudem eine entsprechende Meldung angezeigt.
                    flash('Bitte wählen Sie einen Namen für das neue Attribut, der keine SQL-Schlüsselwörter enthält.')
                    return redirect(url_for('merge_tables'))
        # Anderenfalls wird der Name des neuen Attributs auf None gesetzt.
        else:
            new_attribute_name = None
        try:
            # Aufbau und Ausführung der Abfrage für die Attributübertragung, ohne sie in die Datenbank zu schreiben
            merge_result = simulate_merge_and_build_query(target_table_data, source_table_data, attributes_to_join_on, source_attribute, target_column, cast_direction, new_attribute_name, add_table_names_to_column_names = False)
        except Exception as error:
            # Fehler führen zu einem Abbruch der Operation und der Anzeige einer Fehlermeldung auf der Startseite der Übertragungsfunktion.
            flash(str(error))
            return redirect(url_for('merge_tables'))
        else:
            # Bei Erfolg gibt simulate_merge_and_build_query die aus der Übertragung resultierende Tabelle, die erstellte Abfrage und die für
            # ihre Ausführung benötigten Parameter aus.
            if merge_result is not None and len(merge_result) == 3:
                # Anwendungsbenutzername
                user_name = session['username']
                # Aufteilen des Ergebnisses der Simulation in die Ergebnistabelle, ...
                result = merge_result[0]
                # ... die Abfrage zum Einfügen des neuen Attributs und der Eintragung der neuen Werte ...
                merge_query = merge_result[1]
                # ... sowie des Parameter-Dictionarys für die Abfrage.
                query_parameters = merge_result[2]
                # Wenn das Übertragunsergebnis oder die Abfrage nicht existieren, ist ein Fehler aufgetreten.
                if result is None or merge_query is None:
                    # Dieser wird auf der Startseite der Attributübertragungsfunktion gemeldet.
                    flash('Die Datenbankabfrage für die Vorschau konnte nicht erstellt werden. Bitte versuchen Sie es erneut.')
                    return redirect(url_for('merge_tables'))
                # Wenn kein Name für das neue Attribut spezifiziert wurde, wird hierfür der Name des Quellattributs übernommen.
                if new_attribute_name is None or new_attribute_name == '':
                    new_attribute_name = source_attribute
                ### Beziehen der Tabellenmetadaten für die Anzeige: Datenbanknamen, SQL-Dialekte und Tabellennamen###
                db_name_1 = meta_data_table_1.engine.url.database
                db_name_2 = meta_data_table_2.engine.url.database
                db_dialects = [meta_data_table_1.engine.dialect.name, meta_data_table_2.engine.dialect.name]
                table_name_1 = meta_data_table_1.table_name
                table_name_2 = meta_data_table_2.table_name
                # Die Attributnamen für die Anzeige werden dem CursorResult der simulierten Attributübertragung entnommen.
                table_columns = list(result.keys())
                # Anschließend wird dieses für die leichtere Anzeige in eine Liste von Listen konvertiert.
                data = convert_result_to_list_of_lists(result)
                # Anzeige der Vorschau der Attributübertragung
                return render_template('joined-preview.html', user_name = user_name, db_name_1 = db_name_1, db_name_2 = db_name_2, table_name_1 = table_name_1, table_name_2 = table_name_2, db_dialects = db_dialects, db_name = target_table_data.engine.url.database, table_name = target_table_data.table_name, new_column_name = new_attribute_name, table_columns = table_columns, data = data,  mode = 'merge', target_meta_data_no = target_meta_data_no)
            # Wenn das Ergebnis der Simulation None ist oder nicht die erwartete Form hat, ist ein Fehler aufgetreten.
            else:
                # Daher die Startseite der Attributübertragungsfunktion mit einer Fehlermeldung aufgerufen.
                flash('Die Datenbankabfrage für die Vorschau konnte nicht erstellt werden. Bitte versuchen Sie es erneut.')
                return redirect(url_for('merge_tables'))

### Routen für die Trennung der Verbindungen ###

# Route für die Trennung der Datenbankverbindung
@app.route('/disconnect/<int:engine_no>', methods = ['GET', 'POST'])
def disconnect_from_db(engine_no:int):
    # Ohne Login oder bei Aufruf mittels GET-Request Weiterleitung zur Startseite
    if not session.get('logged_in') or request.method == 'GET':
        return redirect(url_for('start'))
    # Gültiger Auruf nur per POST-Request
    if request.method == 'POST':
        ### Bereitstellung der globalen Variablen, die durch die Abmeldung ggf. verändert werden ###
        global engine_1
        global meta_data_table_1
        global engine_2
        global meta_data_table_2
        global db_in_use
        global tables_in_use
        message = ''
        # Abmeldung von der zuerst angemeldeten Datenbank
        if engine_no == 1:
            # Festlegen der Erfolgsmeldung 
            message = f'Verbindung zur Datenbank {engine_1.url.database} erfolgreich getrennt.'
            # Zurücksetzen der Engine und der Tabellenmetadaten der ersten Tabelle
            engine_1 = None
            meta_data_table_1 = None
            # Falls keine zweite Engine besteht, aber zwei Tabellen gespeichert sind, stammen beide Tabellen aus derselben Datenbank.
            if engine_2 is None and tables_in_use == 3:
                # Daher muss auch das TableMetaData-Objekt der zweiten Tabelle zurückgesetzt werden.
                meta_data_table_2 = None
                # Der Zähler hierfür muss um 2 reduziert werden.
                tables_in_use -= 2
            # Für engine_1 muss der Zähler nur um 1 reduziert werden.
            tables_in_use -= 1
            # Genauso der Zähler für die in Benutzung befindlichen Datenbanken.
            db_in_use -= 1
        # Abmeldung von der als Zweites ausgewählten Datenbank
        elif engine_no == 2:
            # Festlegen der Erfolgsmeldung
            message = f'Verbindung zur Datenbank {engine_2.url.database} erfolgreich getrennt.'
            # Hier werden lediglich alle auf engine_2 bezogenen globalen Variablen zurückgesetzt ...
            engine_2 = None
            meta_data_table_2 = None
            # ... und die Tabellen- und Datenbankzähler jeweils um zwei reduziert.
            tables_in_use -= 2
            db_in_use -= 2
        # Wenn für engine_no der Wert 3 angegeben ist, sollen die Verbindungen zu beiden Datenbanken gleichzeitig getrennt werden.
        elif engine_no == 3:
            # Festlegen der Erfolgsmeldung
            message = f'Verbindung zu den Datenbanken {engine_1.url.database} und {engine_2.url.database} erfolgreich getrennt.'
            ### Zurücksetzen aller auf die Datenbanken bezogenen globalen Variablen ###
            engine_1 = None
            engine_2 = None
            meta_data_table_1 = None
            meta_data_table_2 = None
            tables_in_use = 0
            db_in_use = 0
            global replacement_occurrence_dict
            global replacement_data_dict
            global compatibility_by_code
            global source_attribute
            global target_attribute
            global query_parameters
            global merge_query
            replacement_occurrence_dict= None
            replacement_data_dict= None
            compatibility_by_code= None
            source_attribute= None
            target_attribute= None
            query_parameters= None
            merge_query= None
            engine_no = 1
        # Ausgabe der Nachricht
        flash(message)
        # Weiterleitung zur Datenbankanmeldeseite
        return redirect(url_for('show_db_login_page', engine_no = engine_no))

# Route für die Abmeldung aus der Web-App
@app.route('/logout', methods = ['GET'])
def logout():
    # Ohne Login kein Logout möglich
    if not session.get('logged_in'):
        return redirect(url_for('start'))
    elif request.method == 'GET':
        # Daten der Session entfernen, um den Nutzer auszuloggen
        session.pop('loggedin', None)
        session.pop('username', None)
        # Engines, Hilfsvariablen und Tabellenmetadaten zurücksetzen
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
        # Übermittlung der Benachrichtigung über den erfolgreichen Logout an die nächste Seite der Anwendung
        flash('Sie wurden erfolgreich abgemeldet. \nBitte loggen Sie sich wieder ein, um das Tool zu nutzen.')
        # Weiterleitung zur Login-Seite
        return render_template('login.html')


    

# Ausführung des Programms/Starten des Servers
if __name__ == '__main__':

    maria_engine = create_engine(f'mariadb+pymysql://root:arc-en-ciel@localhost:3306/MariaTest?charset=utf8mb4')
    postgres_engine = create_engine(f'postgresql://postgres:arc-en-ciel@localhost:5432/PostgresTest1', connect_args = {'client_encoding': 'utf8'})
    table_name = 'Vorlesung_Datenbanken_SS2023'
    primary_keys = get_primary_key_from_engine(maria_engine, table_name)
    data_type_info = get_data_type_meta_data(maria_engine, table_name)
    row_count = get_row_count_from_engine(maria_engine, table_name)
    md_table_meta_data_1  = TableMetaData(maria_engine, table_name, primary_keys, data_type_info, row_count)
    primary_keys = get_primary_key_from_engine(postgres_engine, table_name)
    data_type_info = get_data_type_meta_data(postgres_engine, table_name)
    row_count = get_row_count_from_engine(postgres_engine, table_name)
    pg_table_meta_data_1 = TableMetaData(postgres_engine, table_name, primary_keys, data_type_info, row_count)

    yo  = get_replacement_information(md_table_meta_data_1, [('Matrikelnummer', 0), ('Vorname', 1), ('Nachname', 0), ('zugelassen', 0), ('Note', 0)], 'an', 'AHN')[1]
    print(yo)

    # Festlegen des geheimen Schlüssels für die Web-App (aktuell ohne spezifische Funktion, wird jedoch empfohlen)
    app.secret_key = os.urandom(12)
    # Start des Servers auf dem lokalen Rechner unter Portnummer 8000
    serve(app, host = '0.0.0.0', port = 8000)
    