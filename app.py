from flask import Flask, render_template, request, flash, redirect, session, abort, url_for
import os
from waitress import serve
from controller.loginController import login

app = Flask(__name__, template_folder = 'view/templates')

app.secret_key = 'sdfsdlkökl7sedg!'


@app.route('/')
def start(): 
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return index()
    

@app.route('/login', methods=['POST'])
def check_login():
    if (login(request.form['username'], request.form['password']) == True):
        session['logged_in'] = True
        session['username'] = request.form['username']
        return redirect(url_for('index'))
    else:
        flash('wrong password!')
        return start()


@app.route('/index')
def index():
    return render_template('index.html', username = session['username'])


@app.route('/weather')
def get_weather():
    city = request.args.get('city')

    # Check for empty strings or string with only spaces
    if not bool(city.strip()):
        # You could render "City Not Found" instead like we do below
        city = "Kansas City"

    # weather_data = get_current_weather(city)

    # City is not found by API
    # if not weather_data['cod'] == 200:
       # return render_template('city-not-found.html')

    return render_template(
       # "weather.html",
       # title=weather_data["name"],
       # status=weather_data["weather"][0]["description"].capitalize(),
       # temp=f"{weather_data['main']['temp']:.1f}",
       # feels_like=f"{weather_data['main']['feels_like']:.1f}"
    )


if __name__ == '__main__':
    app.secret_key = os.urandom(12)
    serve(app, host = '0.0.0.0', port = 8000)