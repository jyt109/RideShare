from flask import Flask
from flask import request
from flask import render_template
import json
import pandas.io.sql as pdsql
import psycopg2

app = Flask(__name__)
app.shared_route = ''

display_csv_path = 'data/display.csv'
route_tab = 'separate_share_route'

def run_on_start():
    conn = psycopg2.connect(dbname='ridemeter', user='postgres', host='/tmp')
    cursor = conn.cursor()
    return [conn, cursor]


def path_parse(x):
    lst = x.replace('LINESTRING(', '').replace(')', '').split(',')
    return [[item.split(' ')[1], item.split(' ')[0]] for item in lst]


def lst_to_geojson(lst_of_lat_long):
    lst_of_dicts = []
    for ind, (lat, lng) in enumerate(lst_of_lat_long, start=1):
        featDict = {'type': 'Feature'}
        featDict['properties'] = {'id': 'route1', 'time': ind,
                                  'latitude': lat, 'longitude': lng}
        featDict['geometry'] = {"type": "Point", "coordinates": [lng, lat]}
        lst_of_dicts.append(featDict)

    geojson = {'type': 'FeatureCollection',
           'crs': { 'type': 'name',
                    'properties': { 'name': 'urn:ogc:def:crs:OGC:1.3:CRS84'}},
           'features': lst_of_dicts}
    return geojson


@app.route('/readjson', methods=['POST'])
def read_json():
    print 'Loading rides file...'
    d = json.load(open(display_csv_path))
    print d['data'][0]
    return json.dumps(d)


@app.route('/')
def index():
    return render_template('testride.html')


@app.route('/getroutebyid', methods=['POST'])
def getroutebyid():
    data = request.json
    mride, cride = data
    print mride, cride
    q = '''SELECT ST_AsText(sroute) AS s_route,
           ST_AsText(croute) AS c_route,
           ST_AsText(mroute) AS m_route
           FROM %s
           WHERE cride = %s
           AND mride = %s''' % (route_tab, cride, mride)

    routes_pandas = pdsql.read_sql(q, app.conn)
    print 'Got routes'
    rs_route = routes_pandas['s_route'].apply(path_parse).tolist()[0]
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
