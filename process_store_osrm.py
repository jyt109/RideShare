from sql_queries import SqlQueries
import json
import os
import sys


class ProcessStoreOSRM(SqlQueries):
    """Pull OSRM entries from Mongo, decode and put into POSTGIS"""
    def __init__(self, osrm_entries, route_table, route_fpath, route_fname):
        # Instantiate superclass SqlQueries
        super(ProcessStoreOSRM, self).__init__()

        # Generator containing all osrm results, and the count
        self.osrm_entries, self.osrm_cnt = osrm_entries

        # The table name of the routes
        if route_table == 'two_pt':
            self.route_table = self.t_two_pt_route
        if route_table == 'four_pt':
            self.route_table = self.t_four_pt_route

        # The file name of the geojson containing routes
        self.route_fpath = route_fpath
        self.route_fname = route_fname

    def decode_route(self, point_str):
        """INPUT:
        - point_str(STR) [Google encoded geometry of route]

        OUTPUT:
        - points(LIST OF LIST) [List of Lat, Long]

        DOC:
        - Decode goolge encoded route to list of lat, longs
        - Code take from Google Maps API"""

        # Some coordinate offset is represented by 4 to 5 binary chunks
        coord_chunks = [[]]
        for char in point_str:

            # Convert each character to decimal from ascii
            value = ord(char) - 63

            # Values that have a chunk following have an extra 1 on the left
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

            # There is a 1 on the right if the coord is negative
            if coord & 0x1:
                coord = ~coord  # Invert
            coord >>= 1
            coord /= 1000000.0

            coords.append(coord)

        # Convert the 1 dimensional list to a 2 dimensional list and offsets to
        # Actual values
        points = []
        prev_x = 0
        prev_y = 0
        for i in xrange(0, len(coords) - 1, 2):
            if coords[i] == 0 and coords[i + 1] == 0:
                continue

            prev_x += coords[i + 1]
            prev_y += coords[i]
            # A round to 6 digits ensures that the floats are the same as when
            # They were encoded
            points.append((round(prev_x, 6), round(prev_y, 6)))

        return points

    def make_sub_geojson(self, feat_dict):
        """INPUT:
        - sub_feat_dict(DICT) [Summary of route in form of dict]

        OUTPUT:
        - sub_geojson(DICT) [Reformatted to conform to geojson standards]"""

        sub_geojson = {}

        # Decode and delete the key
        # print '\n\n', feat_dict
        # print feat_dict['osrm_route']
        decoded_route = self.decode_route(feat_dict['osrm_route'])
        del feat_dict['osrm_route']

        # Delete the id the Mongo assigns
        del feat_dict['_id']

        # Recast into a sub geojson object
        sub_geojson['geometry'] = {'coordinates': decoded_route,
                                   'type': 'LineString'}
        sub_geojson['properties'] = feat_dict

        return sub_geojson

    def make_geojson(self):
        """INPUT:
        - self.osrm_entries(GENERATOR) [All OSRM results]
        - self.osrm_cnt(INT) [The number of OSRM results]

        OUTPUT:
        - NONE [Geojson written to file]

        DOC:
        - Convert the OSRM results to a geojson file then to sql later"""

        # Variables from __init__
        rows = self.osrm_entries
        count = self.osrm_cnt

        geojson = {}
        geojson['type'] = 'FeatureCollection'
        geojson['features'] = []

        for ind, feat_dict in enumerate(rows, start=1):
            # Keeping tabs on the percentage done
            if ind % 1000 == 0 or ind == 100:
                print 'Done converting %d entries (%d%%)' % \
                      (ind, float(ind) / count * 100)

            # Make the sub_geojsons and add to the bigger geojson list
            sub_geojson = self.make_sub_geojson(feat_dict)
            geojson['features'].append(sub_geojson)

        print 'Dumping GEOJON into file...'
        route_fullpath = os.path.join(self.route_fpath, self.route_fname)
        route_fhandle = open(route_fullpath, 'w')
        route_fhandle.write(json.dumps(geojson))

    def load_geojson_to_db(self, drop_original):
        """INPUT:
        - drop_original(BOOL) [Drop the original table when loading script]

        OUTPUT:
        - NONE

        DOC:
        - Load route geojson into db after converting to shp then sql"""

        self.convert_geojson_to_sql(self.route_fpath, self.route_fname,
                                    self.route_table, single_geom=True)
        self.load_sql_script(drop_original)
