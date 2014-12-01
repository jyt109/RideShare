import os
import sys
import requests
import json
import subprocess
from sql_queries import SqlQueries


class QueryOSRM(SqlQueries):
    """INPUT:
    - mongo_inst_tab(MONGO TABLE VAR) [From the MongoUtil Class]

    OUTPUT:
    - NONE

    DOC:
    - Query OSRM and write results to MongoDB
    - Return all the entries written to MongoDB"""

    def __init__(self, mongo_inst_tab, custom_osrm=None,
                 port=6969, launch=True):
        # Instantiating SqlQueries
        super(QueryOSRM, self).__init__()

        # Taking in a mongo table variable so we can insert into the table
        self.mongo_inst_tab = mongo_inst_tab

        # OSRM path
        if not custom_osrm:
            self.osrm_path = 'osrm-backend/build'

        # The port OSRM API is going to running on (locally)
        self.port = port

        # Get the OSRM Engine running
        if launch:
            self.start_osrm_server()

    def start_osrm_server(self):
        """INPUT:
        - NONE

        OUTPUT:
        - NONE

        DOC:
        - Go to where OSRM is and start the server"""

        print 'Starting OSRM Server...'
        os.chdir(self.osrm_path)
        osrm_cmd = './osrm-routed new-york-latest.osrm -p %d&' % self.port
        subprocess.call(osrm_cmd, shell=True)
        print 'OSRM Server Running...'

    def osrm_link_two_input(self):
        """INPUT:
        - NONE

        OUTPUT:
        - (GENERATOR) [Rows containing all Lat, Long pairs to be used in OSRM]

        DOC:
        - Go to where OSRM is and start the server"""

        return self.sql_osrm_two_lat_long()

    def run_two_locations(self):
        """INPUT:
        - NONE

        OUTPUT:
        - (GENERATOR) [Rows containing all Lat, Long pairs to be used in OSRM]

        DOC:
        - Go to where OSRM is and start the server"""

        # Get the lat,long input for two locations
        ride_pick_drop = self.osrm_link_two_input()

        # Template link for the lat,long input
        link = '''http://localhost:%s/viaroute?loc=%s&loc=%s'''

        # Creating the links and running them
        for ind, row in enumerate(ride_pick_drop, start=1):

            # Keep track of how many run
            if ind % 1000 == 0 or ind == 10 or ind == 100:
                print 'Inserted into Mongo: %d OSRM entries ...' % ind

            ride, p_latlong, d_latlong = row
            full_link = link % (self.port, p_latlong, d_latlong)

            response = requests.get(full_link)

            # Get the reformatted features
            feat_dict = self.osrm_response_to_dict(response, [ride], 0)

            #Insert into MongoDB
            if feat_dict:
                self.mongo_inst_tab.insert(feat_dict)

    def concat_osrm_response_summary(self, response):
        """INPUT:
        - response(REQUESTS RESPONSE OBJ) [Query response from OSRM]

        OUTPUT:
        - (TUPLE) [TUPLE of route summaries(dict) list and geom list]

        DOC:
        - Pool all the routes found by OSRM for downstream processing"""

        # Check if the response is OK
        if response.status_code == 200:
            osrm_dict = json.loads(response.content)

            # Check if a route is found
            if osrm_dict['status'] == 0:
                summary = osrm_dict['route_summary']
                geometry = osrm_dict['route_geometry']

                # Alternative routes by default empty
                alt_summary = []
                alt_geometry = []
                if 'alternative_summaries' in osrm_dict.keys() and \
                   'alternative_geometries' in osrm_dict.keys():
                    # If alternative routes, list of dicts
                    alt_summary = osrm_dict['alternative_summaries']
                    # Also list of geometry
                    alt_geometry = osrm_dict['alternative_geometries']

                # List of summaries and geometries for all routes
                alt_summary.append(summary)
                alt_geometry.append(geometry)

                return (alt_summary, alt_geometry)
        # Empty list if no routes found or error
        return ()

    def find_shortest_route(self, summary_geometry):
        """INPUT:
        - summary_geometry(TUPLE) [List of summary and List of geom in a tup]

        OUTPUT:
        - (TUPLE) [Summary(DICT), geometry(STR) for the shortest path]

        DOC:
        - Find the shortest path given a list of routes"""

        if summary_geometry:
            #Unpack the tuple
            summary_lst, geometry_lst = summary_geometry

            if len(summary_lst) > 1:
                # Initialize the index of the shortest route
                best_route_ind = -1
                # Distance that surpass any possible route
                best_route_dist = 1e50

                # Loop through all the routes to find the shortest
                for ind, summary_dict in enumerate(summary_lst):
                    dist = summary_dict['total_distance']
                    if dist != 0 and dist < best_route_dist:
                        best_route_ind = ind

                # Return the summary and geometry of the shortest
                if best_route_ind != -1:
                    best_summary = summary_lst[best_route_ind]
                    best_geometry = geometry_lst[best_route_ind]
                    return (best_summary, best_geometry)
                else:
                    print 'find_shortest_route() failed... Exiting...'
                    sys.exit()
            else:
                # Else the best route is the only route
                return (summary_lst[0], geometry_lst[0])
        # Empty list if no routes found
        return (None, None)

    def osrm_response_to_dict(self, response, ride_ids, ride_type):
        """INPUT:
        - response(REQUESTS RESPONSE OBJ) [Query response from OSRM]
        - ride_ids(LIST) [The ride_id for the particular pick and drop]

        OUTPUT:
        - (DICT) [Information we need from OSRM to put into Mongo]"""

        feat_dict = {}

        # Get the info for the shortest path
        sum_geom = self.concat_osrm_response_summary(response)
        short_summary, short_geometry = self.find_shortest_route(sum_geom)

        # If there is a route, reformat the summary
        if short_summary and short_geometry:
            feat_dict['osrm_time'] = short_summary['total_time']
            feat_dict['osrm_dist'] = short_summary['total_distance']
            feat_dict['osrm_start'] = short_summary['start_point']
            feat_dict['osrm_end'] = short_summary['end_point']
            feat_dict['osrm_route'] = short_geometry

            # Between one set of pickup and dropoff points
            if len(ride_ids) == 1:
                feat_dict['ride'] = ride_ids[0]
                # Should be 0 since there is no ride share
                feat_dict['rs_type'] = ride_type

            # Between two sets of pickup and dropoff points
            if len(ride_ids) == 2:
                cride, mride = ride_ids
                feat_dict['mride'] = mride
                feat_dict['cride'] = cride
                # Should be 1/ 2 depending which one get dropped first
                feat_dict['rs_type'] = ride_type

        return feat_dict

    def compare_two_routes(self, route_1, route_2):
        """INPUT:
        - route_1(DICT) [feat_dict of first route]
        - route_2(DICT) [feat_dict of second route]

        OUTPUT:
        - (DICT) [Information we need from OSRM to put into Mongo]"""

        if route_1['osrm_dist'] > route_2['osrm_dist']:
            best = route_2
        elif route_2['osrm_dist'] > route_1['osrm_dist']:
            best = route_1
        else:
            # If equal distance, return route_1
            best = route_1

        return best
