from flask import Flask, request, render_template, url_for
import requests
import json
import pandas as pd
import pandas.io.sql as pdsql
import os
import psycopg2
#from model import getdata, myModel
# from convert2geojson import convert2geojson


app = Flask(__name__)
app.shared_route = ''


def run_on_start():
    conn = psycopg2.connect(dbname='ride_viz', user='postgres', host='/tmp')
    cursor = conn.cursor()
    return [conn, cursor]

def path_parse(x):
    lst = x.replace('LINESTRING(', '').replace(')', '').split(',')
    return [[item.split(' ')[1], item.split(' ')[0]] for item in lst] 

def lst_to_geojson(lst_of_lat_long):
    lst_of_dicts = []
    for ind, (lat, lng) in enumerate(lst_of_lat_long, start=1):
        featDict = {'type': 'Feature'}
        featDict['properties'] = {'id': 'route1', 'time': ind, 'latitude': lat, 'longitude': lng}
        featDict['geometry'] = {"type": "Point", "coordinates": [lng, lat]}
        lst_of_dicts.append(featDict)

    geojson = {'type': 'FeatureCollection',
           'crs': { 'type': 'name', 'properties': { 'name': 'urn:ogc:def:crs:OGC:1.3:CRS84'}},
           'features': lst_of_dicts}
    return geojson


@app.route('/')
def index():
    lst = range(10)
    return render_template('index.html', entries=lst)

@app.route('/readjson', methods=['POST'])
def read_json():
    print 'Loading rides file...'
    d = json.load(open('data/testing.json'))
    return json.dumps(d)

@app.route('/testshare')
def testshare():
    return render_template('testride.html')

@app.route('/getroutebyid', methods=['POST'])
def getroutebyid():
    data = request.json
    mride, cride = data
    print mride, cride
    q = '''SELECT ST_AsText(geom) AS shared_route,
           ST_AsText(cpath) AS c_route,
           ST_AsText(mpath) AS m_route 
           FROM rs_figures
           WHERE %s = c_ride
           AND %s  =  mride''' % (cride, mride)
    routes_pandas = pdsql.read_sql(q, app.conn)
    print routes_pandas
    rs_route = routes_pandas['shared_route'].apply(path_parse).tolist()[0]
    rs_route_json = lst_to_geojson(rs_route)
    app.shared_route = rs_route_json
    c_route = routes_pandas['c_route'].apply(path_parse).tolist()[0]
    m_route = routes_pandas['m_route'].apply(path_parse).tolist()[0]
    return json.dumps([c_route, m_route])

@app.route('/return_ride_share', methods=['GET'])
def return_ride_share():
    print 'return ride share called'
    return json.dumps(app.shared_route)

if __name__ == '__main__':
    app.conn, app.cursor = run_on_start()
    app.run(port=8000, debug=True)
