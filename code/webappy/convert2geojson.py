import json
import os
from decode_route import decode

def subGeoJson(d):
    lst_of_dicts = []
    lst_of_longLat = decode(d['route_geometry'])
    for ind, (rawLng, rawLat) in enumerate(lst_of_longLat, start=1):
        featDict = {'type': 'Feature'}
        lat = rawLat / 10.
        lng = rawLng / 10.
        featDict['properties'] = {'id': 'route1', 'time': ind, 'latitude': lat, 'longitude': lng}
        featDict['geometry'] = {"type": "Point", "coordinates": [lng, lat]}
        lst_of_dicts.append(featDict)
    return lst_of_dicts


def convert2geojson(d):
    geojson = {'type': 'FeatureCollection',
               'crs': { 'type': 'name', 'properties': { 'name': 'urn:ogc:def:crs:OGC:1.3:CRS84'}},
               'features': subGeoJson(d)}
    return geojson

    