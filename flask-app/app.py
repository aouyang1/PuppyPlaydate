__author__ = 'aouyang1'

from flask import Flask, render_template, redirect, url_for, request
from cassandra.cluster import Cluster

app = Flask(__name__)
cluster = Cluster(['54.215.184.69'])
session = cluster.connect('test')

@app.route('/')
def home():
    return "Hello World"


@app.route('/welcome')
def welcome():
    return render_template("welcome.html")


@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form['username'] != "admin" or request.form['password'] != 'admin':
            error = 'Invalid credentials'
        else:
            return redirect(url_for('home'))
    return render_template("login.html", error=error)


@app.route('/county/')
def county_full():
    county_full = session.execute('SELECT * FROM by_county_full')
    counties = ""
    for row in county_full:
        counties += row.county + ": " + str(row.cnt) + "<br>"

    return counties

@app.route('/monthly/<county>/')
def county_month(county):
    county_month = session.execute("SELECT * FROM by_county_month WHERE county = '" + county + "'")
    counties_month = ""
    for row in county_month:
        counties_month += row.county + ": (" + str(row.year) + "," + str(row.month) + ") " + str(row.cnt) + "<br>"
    return counties_month

@app.route('/daily/<county>/')
def county_day(county):   
    county_day = session.execute("SELECT * FROM by_county_day WHERE county = '" + county + "'")
    counties_day = ""
    for row in county_day:
        counties_day += row.county + ": (" + str(row.year) + "," + str(row.month) + "," + str(row.day) + ") " + str(row.cnt) + "<br>"
    return counties_day


if __name__ == '__main__':
    app.run(debug=True)
