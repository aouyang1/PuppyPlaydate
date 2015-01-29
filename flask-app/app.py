__author__ = 'aouyang1'

from flask import Flask, render_template, redirect, url_for, request, jsonify
from cassandra.cluster import Cluster
import csv
import time
import pandas as pd
import numpy as np
import time

app = Flask(__name__)
cluster = Cluster(['54.215.184.69'])
session = cluster.connect('puppy')


county_code_file = open('county_codes.csv','rb')
wr = csv.reader(county_code_file)
codes = [code for code in wr][0]

county_name_file = open('county_names.csv','rb')
wr = csv.reader(county_name_file)
names = [name for name in wr][0]

county_code_dict = {}
for name, code in zip(names, codes):
    county_code_dict[name] = code

code_county_dict = {code: name for name, code in county_code_dict.items()}

@app.route('/')
def home():
    return "Hello World"


@app.route('/welcome')
def welcome():
    return render_template("welcome.html")

@app.route('/graph_ex')
def index(chartID = 'chart_ID', chart_type = 'bar', chart_height = 350):
    chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
    series = [{"name": 'Label1', "data": [1,2,3]}, {"name": 'Label2', "data": [4, 5, 6]}]
    title = {"text": 'My Title'}
    xAxis = {"categories": ['xAxis Data1', 'xAxis Data2', 'xAxis Data3']}
    yAxis = {"title": {"text": 'yAxis Label'}}
    return render_template('index.html', chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis)


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
    start_time = time.time()
    county_full = session.execute('SELECT * FROM by_county_full')
    print time.time() - start_time

    start_time = time.time()
    counties = ""    
    for row in county_full:
        counties += row.state + "," + row.county + ": " + str(row.count) + "<br>"
    print time.time() - start_time

    return counties


@app.route('/update_map/')
def update_map():
    return "MEH"


@app.route('/update_chart/<county_code>/')
def update_chart(county_code):

    county = code_county_dict[county_code]
    county_state = [county_attr.strip() for county_attr in county.split(",")]

    county = county_state[0]
    state = county_state[1]

    county_month = session.execute("SELECT * FROM by_county_month WHERE state = '" + state + "' AND county = '" + county + "'")

    def date_to_milli(time_tuple):
        epoch_sec = time.mktime((1970, 1, 1, 0, 0, 0, 0, 0, 0))
        return 1000*int(time.mktime(time_tuple) - epoch_sec)

    historical_data = []
    for row in county_month:
        curr_date = row.date
        year = curr_date/100
        month = curr_date - year*100
        historical_data.append([date_to_milli((year, month, 0, 0, 0, 0, 0, 0, 0)), row.count])

    return jsonify(state=state, county=county, historical_data=historical_data)


@app.route('/monthly/')
def county_month(county="Dallas County", state="TX"):

    county = "Denton County"
    state = "TX"

    county_month = session.execute("SELECT * FROM by_county_month WHERE state = '" + state + "' AND county = '" + county + "'")

    def date_to_milli(time_tuple):
        epoch_sec = time.mktime((1970, 1, 1, 0, 0, 0, 0, 0, 0))
        return 1000*int(time.mktime(time_tuple) - epoch_sec)

    historical_data = []
    for row in county_month:
        curr_date = row.date
        year = curr_date/100
        month = curr_date - year*100
        historical_data.append([date_to_milli((year, month, 0, 0, 0, 0, 0, 0, 0)), row.count])


    county_code_file = open('county_codes.csv','rb')
    wr = csv.reader(county_code_file)
    codes = [code for code in wr][0]

    county_name_file = open('county_names.csv','rb')
    wr = csv.reader(county_name_file)
    names = [name for name in wr][0]

    county_dict = {}
    for name, code in zip(names, codes):
        county_dict[name] = code



    return render_template('index.html', state=state, county=county, historical_data=historical_data)


@app.route('/daily/<county>/')
def county_day(county):   
    start_time = time.time()
    county_day = session.execute("SELECT * FROM by_county_day WHERE county = '" + county + "'")
    print time.time() - start_time

    start_time = time.time()
    counties_day = ""    
    for row in county_day:
        counties_day += row.county + ": (" + str(row.year) + "," + str(row.month) + "," + str(row.day) + ") " + str(row.cnt) + "<br>"
    print time.time() - start_time

    return counties_day

@app.route('/hourly/<county>/')
def county_hour(county):  
    start_time = time.time()
    county_hour = session.execute("SELECT * FROM by_county_hour WHERE county = '" + county + "'")
    print time.time() - start_time

    start_time = time.time()
    counties_hour = ""    
    for row in county_hour:
        counties_hour += row.county + ": (" + str(row.year) + "," + str(row.month) + "," + str(row.day) + "," + str(row.hour) + ") " + str(row.cnt) + "<br>"
    print time.time() - start_time

    return counties_hour


if __name__ == '__main__':
    app.run(debug=True)
