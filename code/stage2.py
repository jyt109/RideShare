import subprocess
import os
import requests
import json
from time import time
from pymongo import MongoClient
import psycopg2
from psqlSetup import DBRideShare
import sys

class QueryOSRM(DBRideShare):
    def __init__(self, osrmPath='default', port=6969, 
                 mong_db='ride_routes',
                 mong_tab='ride_route_tab',
                 geojson_fname='geo_routes.json',
                 tab_name='routes'):
        super(QueryOSRM, self).__init__() #self.conn
        if osrmPath == 'default':
            self.osrmPath = '/Users/JeffreyTang/Desktop/capstones/ride_sharing/osrm-backend/build'
        self.port = port
        self.mong_db = mong_db
        self.mong_tab = mong_tab
        self.geojson_fname = geojson_fname
        self.tab_name = tab_name

    def launchMongo(self):
        print 'Launching Mongo...'
        c = MongoClient()
        print 'DATABASE AND TABLE', self.mong_db, self.mong_tab
        db = c[self.mong_db]
        tab = db[self.mong_tab]
        return tab
    
    def startOSRMServer(self):
        print 'Starting OSRM Server...'
        os.chdir(self.osrmPath)
        subprocess.call('./osrm-routed new-york-latest.osrm -p %d&' % self.port, shell=True)
        print 'OSRM Server Running...'

    def osrmInput(self):
        return super(QueryOSRM, self).sql4osrm(execute=True)

    def response2dict(self, response, keys, key=1):
        feat_dict = {}
        if response.status_code == 200:
            d = json.loads(response.content)
            if d['status'] == 0: #route found
                summary = d['route_summary']
                if summary['total_time'] == 0 or summary['total_distance'] == 0:
                    pass
                else:
                    feat_dict['osrm_time'] = summary['total_time']
                    feat_dict['osrm_distance'] = summary['total_distance']
                    feat_dict['osrm_route'] = d['route_geometry'] #coded
                    feat_dict['osrm_start'] = summary['start_point']
                    feat_dict['osrm_end'] = summary['end_point']
                    if key == 1:
                        ride = keys[0]
                        feat_dict['ride'] = ride
                    elif key == 2:
                        cride, mride = keys
                        feat_dict['mride'] = mride
                        feat_dict['cride'] = cride
        return feat_dict


    def run(self, startOSRM=True):
        if startOSRM:
            self.startOSRMServer()
        tab = self.launchMongo()
        osrm_root = '''http://localhost:%s/viaroute?loc=%s&loc=%s'''
        ride_pd_rows = self.osrmInput()
        for ind, row in enumerate(ride_pd_rows, start=1):
            if ind % 1000 == 0 or ind == 10 or ind == 100:
                print 'Done: %d OSRM entries ...' % ind
            ride, p_latlong, d_latlong = row
            url = osrm_root % (p_latlong, d_latlong)
            response = requests.get(url)
            feat_dict = self.response2dict(response, [ride])
            tab.insert(feat_dict)

    def runTwo(self, startOSRM=True):
        if startOSRM:
            self.startOSRMServer()
        tab = self.launchMongo()
        osrm_root = '''http://localhost:%s/viaroute?loc=%s&loc=%s&loc=%s&loc=%s'''
        ride_pd_rows = self.osrmInput()
        for ind, row in enumerate(ride_pd_rows, start=1):
            if ind % 1000 == 0 or ind == 10 or ind == 100:
                print 'Done: %d OSRM entries ...' % ind
            cride, mride =  row[:2]   
            cp_latlong, cd_latlong, mp_latlong, md_latlong = [item.replace(' ','') for item in row[2:]]
            url_1 = osrm_root % (self.port, cp_latlong, mp_latlong, cd_latlong, md_latlong)
            url_2 = osrm_root % (self.port, cp_latlong, mp_latlong, md_latlong, cd_latlong)
            response1 = requests.get(url_1)
            response2 = requests.get(url_2)
            feat_dict1 = self.response2dict(response1, [cride, mride], key=2)
            feat_dict2 = self.response2dict(response2, [cride, mride], key=2)
            if feat_dict1 and feat_dict2:
                if feat_dict1['osrm_time'] > feat_dict2['osrm_time']:
                    feat_dict2['ride_share_type'] = '2'
                    tab.insert(feat_dict2)
                else:
                    feat_dict1['ride_share_type'] = '1'
                    tab.insert(feat_dict1)
            elif feat_dict1 and not feat_dict2:
                feat_dict1['ride_share_type'] = '1'
                tab.insert(feat_dict1)
            elif feat_dict2 and not feat_dict1:
                feat_dict2['ride_share_type'] = '2'
                tab.insert(feat_dict2)


    def extractFromMongo(self):
        '''
        INPUT: None
        OUTPUT: Generator of dicts containing route info(One dict = One route), count (INT)
        '''
        tab = self.launchMongo()
        return (tab.find(), tab.count())

    def decode(self, point_str):
        '''
        Google decoding route 
        '''
        # sone coordinate offset is represented by 4 to 5 binary chunks
        coord_chunks = [[]]
        for char in point_str:

            # convert each character to decimal from ascii
            value = ord(char) - 63

            # values that have a chunk following have an extra 1 on the left
            split_after = not (value & 0x20)
            value &= 0x1F

            coord_chunks[-1].append(value)

            if split_after:
                coord_chunks.append([])

        del coord_chunks[-1]

        coords = []

        for coord_chunk in coord_chunks:
            coord = 0

            for i, chunk in enumerate(coord_chunk):
                coord |= chunk << (i * 5)

            #there is a 1 on the right if the coord is negative
            if coord & 0x1:
                coord = ~coord #invert
            coord >>= 1
            coord /= 1000000.0

            coords.append(coord)

        # convert the 1 dimensional list to a 2 dimensional list and offsets to
        # actual values
        points = []
        prev_x = 0
        prev_y = 0
        for i in xrange(0, len(coords) - 1, 2):
            if coords[i] == 0 and coords[i + 1] == 0:
                continue

            prev_x += coords[i + 1]
            prev_y += coords[i]
            # a round to 6 digits ensures that the floats are the same as when
            # they were encoded
            points.append((round(prev_x, 6), round(prev_y, 6)))

        return points

    def subGeoJson(self, d, key=1):
        '''
        INPUT: dict (OSRM JSON FORMAT)
        OUTPUT: dict (GEOJSON FORMAT)
        '''
        featDict = {}
        decoded_route = self.decode(d['osrm_route'])
        # print decoded_route[0]
        featDict['geometry'] = dict(coordinates=decoded_route, type='LineString')
        if key == 1:
            featDict['properties'] = dict(osrm_start=d['osrm_start'],
                                          osrm_end=d['osrm_end'],
                                          osrm_time=d['osrm_time'],
                                          osrm_dist=d['osrm_distance'],
                                          ride=d['ride'])
        elif key == 2:
            featDict['properties'] = dict(osrm_start=d['osrm_start'],
                              osrm_end=d['osrm_end'],
                              osrm_time=d['osrm_time'],
                              osrm_dist=d['osrm_distance'],
                              mride=d['mride'],
                              c_ride=d['cride'],
                              rs_type=d['ride_share_type'])
            
            if 'rs_type' not in featDict['properties'].keys():
                print featDict['properties'].keys()
                print "STOP"
                sys.exit(1)


        return featDict

    def convertMultiGeoJson(self, gd, count=None, key=1):
        '''
        INPUT: Generator of dicts (From Mongo)
        OUTPUT: GEOJSON File
        '''
        geojson = {}
        geojson['type'] = 'FeatureCollection'
        geojson['features'] = []

        for ind, d in enumerate(gd, start=1):
            if ind % 1000 == 0 or ind == 1 or ind == 2 or ind == 100:
                print 'Done converting %d entries (%d%%)' % (ind, float(ind) / count * 100)

            featDict = self.subGeoJson(d, key=key)
            geojson['features'].append(featDict)

        print 'Dumping GEOJON into file...'
        f = open(self.geojson_fname, 'w')
        f.write(json.dumps(geojson))


    def pushGeojsonInDB(self):
        super(QueryOSRM, self).loadInSql(self.geojson_fname, singleGeom=True, tab_name=self.tab_name)

    def joinDistrictOSRM(self):
        super(QueryOSRM, self).sql4JoinAll(execute=True)

    def osrm2geoson2sql(self, startOSRM=False, run=1):
        print 'Running OSRM and dumping into Mongo...'
        if run == 1:
            self.run(startOSRM=startOSRM)
        elif run == 2:
            self.runTwo(startOSRM=startOSRM)
        print 'Extracting from Mongo...'
        gd, cnt = self.extractFromMongo()
        print 'Converting to geojson...'
        self.convertMultiGeoJson(gd, count=cnt, key=run)
        print 'Pushing into DB...'
        self.pushGeojsonInDB() 

if __name__ == '__main__':
    ###EACH TIME WE RE-RUN THIS, HAVE TO REMOVE THESE TABLES
    #drop table pickup_district;
    #drop table dropoff_district;
    #drop table rides_11_4_8_all;
    #drop table rides_11_4_8_osrm;

    # Prepare data for OSRM query
    dbrs = DBRideShare()
    # loading in sql script of district data
    dbrs.loadInSql('nyc_neighborhoods.sql')
    # Get district info and store into psql 
    dbrs.sql4matchDistrict(execute=True)


    # All data ready for OSRM query
    osrm = QueryOSRM()
    osrm.run(startOSRM=False)
    gd, cnt = osrm.extractFromMongo()
    osrm.convertMultiGeoJson(gd, count=cnt)
    # Get route Data and store into psql
    osrm.pushGeojsonInDB() 
    osrm.joinDistrictOSRM()







