import psycopg2
from sqlalchemy import create_engine
import subprocess, os
from psycopg2 import ProgrammingError
import pandas as pd
import time, datetime
import pandas.io.sql as pdsql
import json 

class DBFunction(object):
    '''
    This is a class to setup the database
    And contains basic access info for the db
    '''
    def __init__(self,
                db_name='ride_viz',
                user='postgres', 
                t_ride='ride', 
                t_ride_dist='ride_district',
                t_route='route', 
                t_dist_route='ride_district_route',
                first_filter_tab='first_filter', 
                params_tab='rs_params',
                filtered_ride_tab='filtered_rides',
                osrm_pred='evaluation_osrm',
                evaluation_tab='osrm_evaluation'):

        # Database Credentials
        self.db_name = db_name
        self.user = user
        self.engine = create_engine('postgresql://:@localhost/%s' % db_name)
        self.conn = psycopg2.connect(dbname=self.db_name, user=self.user, host='/tmp')

        self.cursor = self.conn.cursor()
        self.t_ride = t_ride
        self.t_ride_dist = t_ride_dist
        self.t_route = t_route
        self.t_dist_route = t_dist_route
        self.first_filter_tab = first_filter_tab
        self.params_tab = params_tab
        self.filtered_ride_tab = filtered_ride_tab
        self.osrm_pred = osrm_pred,
        self.evaluation_tab = evaluation_tab

    def sql4createDB(self, execute=False):
        '''
        Think of how to move this into python
        stage 0 stuff
        '''
        # psql -U postgres
        # CREATE DATABASE ride_viz;
        # psql ride_viz
        # CREATE EXTENSION postgis;
        pass


    def geojson2shp2sql(self, fname_json, fname_shp, table_name, fname_sql, singleGeom=True):
        print 'geojson to shp...'
        subprocess.call('ogr2ogr -progress -f "ESRI Shapefile" %s %s OGRGeoJSON' % (fname_shp, fname_json), shell=True)
        print 'shp to sql...'
        #-S give us LineString instead of MultiLineString
        if singleGeom:
            subprocess.call('shp2pgsql -I -s 4326 -S %s %s > %s' % (fname_shp, table_name, fname_sql), shell=True)
        else:
            subprocess.call('shp2pgsql -I -s 4326 %s %s > %s' % (fname_shp, table_name, fname_sql), shell=True)
        os.rename(fname_sql, fname_sql)  #'db/' +     
        
    def loadInSql(self, fname, singleGeom=True, tab_name='routes'):
        if fname.endswith('.json'):
            raw_fname = fname.strip('.json').replace('.', '')
            fname_shp = raw_fname + '.shp'
            fname_sql = raw_fname + '.sql'
            if tab_name == 'district':
                self.geojson2shp2sql(fname, fname_shp, self.rides_district_tab, fname_sql, singleGeom=singleGeom)
            elif tab_name == 'routes':
                self.geojson2shp2sql(fname, fname_shp, self.routes_tab, fname_sql, singleGeom=singleGeom)
            elif tab_name == 'osrm_pred':
                self.geojson2shp2sql(fname, fname_shp, self.osrm_pred, fname_sql, singleGeom=singleGeom)
            else:
                self.geojson2shp2sql(fname, fname_shp, tab_name, fname_sql, singleGeom=singleGeom)
            fname = fname_sql
        print 'Calling %s SQL Script...' % tab_name
        try:
            self.cursor.execute(open(fname).read())
            self.conn.commit()
        except ProgrammingError as e:
            print e
            self.conn.rollback()


    ####  //// Start of sql utility functions //// ####
    def sql_create_index(table_name, index_col):
        '''
        INPUT: STRING, LIST
        OUTPUT: STRING(Query)

        - Template SQL for creating index
        '''

        index_str = ','.join(index_col)
        return '''CREATE UNIQUE INDEX %s_ind ON %s (%s);''' % (table_name, table_name, index_col)

    def execute_q(self, q, execute=True , fetch=False, msg=None):
        if execute:
            if msg:
                print msg
            else:
                print 'Executing query...'
            try:
                self.cursor.execute(q)
                if fetch:
                    return self.cursor.fetchall()
                self.conn.commit()
            except ProgrammingError as e:
                print e 
                self.conn.rollback()
            return None
        else:
            print 'Query requested. Query returned'
            return q
    #### //// End of sql utility functions //// ####


    ####  //// Start of sql query functions //// ####
    def sql_rides_district(self, execute=True):
        '''
        INPUT: BOOL(OPTIONAL)
        OUTPUT: LIST(Queries) OR None

        - Join rides with their corresponding pickup and dropoff district
        - Use the ST_Within() to check if a point is within an area
        - If execute is True then return None, else the queries
        '''

        # Make Pickup District table with rides
        q1 = \
        '''CREATE TABLE  pickup_district AS (
            SELECT r.*, nb.city AS pcity, nb.name AS pname, 
            ST_SetSRID(ST_MakePoint(r.pickup_longitude, r.pickup_latitude),4326) AS pickup_geom
            FROM %s AS r
            CROSS JOIN neighborhood_bounds AS nb
            WHERE ST_Within(ST_SetSRID(ST_MakePoint(r.pickup_longitude, r.pickup_latitude),4326), nb.geom)
            );
        ''' % self.t_ride # ride

        # Make Dropoff District table with rides
        q2 = \
        '''CREATE TABLE dropoff_district AS (
            SELECT r.ride, nb.city AS dcity, nb.name AS dname, 
            ST_SetSRID(ST_MakePoint(r.dropoff_longitude, r.dropoff_latitude),4326) AS dropoff_geom
            FROM %s AS r
            CROSS JOIN neighborhood_bounds AS nb
            WHERE ST_Within(ST_SetSRID(ST_MakePoint(r.dropoff_longitude, r.dropoff_latitude),4326), nb.geom)
            );
        ''' % self.t_ride # ride

        # Joining the previous two table to make a combined table
        q3 = \
        '''CREATE TABLE %s AS (
            SELECT p.*, d.dcity, d.dname, d.dropoff_geom
            FROM pickup_district AS p
            INNER JOIN dropoff_district AS d
            ON p.ride = d.ride
           );
        ''' % self.t_ride_dist # ride_district

        # Executing the queries
        q1_done = self.execute_q(q1, execute=execute, msg='Creating Pickup District Table...')
        q2_done = self.execute_q(q2, execute=execute, msg='Creating Dropoff District Table...')
        q3_done = self.execute_q(q3, execute=execute, msg='Creating Pickup and Dropoff District Joint Table...')
        if execute:
            return None
        else:
            return [q1_done, q2_done, q3_done]


    def sql_osrm_lat_long(self, execute=True):
        '''
        INPUT: BOOL(OPTIONAL)
        OUTPUT: STRING OR GENERATOR(Query Results)

        - OSRM queries need pickup and dropoff locations as LineString
        - Latitude,Longitude (Note there must be no space between the two)
        - Route table will be loaded in by loadInSql() by converting shp file from OSRM
        '''

        # select pickup and dropoff lat,long
        q = \
        '''SELECT ride, CONCAT_WS(',', pickup_latitude, pickup_longitude),
           CONCAT_WS(',',dropoff_latitude, dropoff_longitude)
           FROM %s
        ''' % self.t_ride

        # Executing the query
        q_done = self.executeQ(q, execute=execute, msg='Getting pickup and dropoff coordinates...')
        if execute:
            return self.cursor.fetchall()
        else:
            return q_done

    def sql_ride_district_route(self, execute=True):
        '''
        INPUT: BOOL(OPTIONAL)
        OUTPUT: STRING OR None

        - Merge ride, district and route to make one table
        '''

        # creating the combined table (ride, district, route)
        # exclude if there are more than one route (small number of rouge results)
        q1 = \
        '''CREATE TABLE %s AS (
            SELECT dist_ride.*, rou.osrm_time, rou.osrm_dist, 
            rou.osrm_start, rou.osrm_end, rou.geom
            FROM %s AS rou
            JOIN %s AS dist_ride
            ON rou.ride = dist_ride.ride AND
            rou.ride NOT IN (SELECT ride AS cnt FROM %s GROUP BY ride having COUNT(*) > 1));
        ''' % (self.t_dist_route, self.t_route, self.t_ride_dist, self.t_route)

        # making ride as index
        q2 = sql_create_index(self.t_dist_route, ['ride'])


        if execute:
            print 'Creating Joint Table ...'
            self.executeQ(q1, execute=execute, msg='Ride + District Joining Route')
            self.executeQ(q2, execute=execute, msg='Creating Index')
        else: 
            return (q1, q2)

    def sql4RSFilter1(self, execute=True):
        '''
        INPUT: BOOL(OPTIONAL)
        OUTPUT: STRING OR None

        - Merge ride, district and route to make one table
        '''


        q = '''SELECT ride, pickup_datetime, pcity
            FROM %s
            WHERE trip_time_in_secs > 60 AND
            osrm_time > 60 
            ORDER BY pickup_datetime, dropoff_datetime;''' % self.all_tab
        df = pdsql.read_sql(q, self.conn)
        return df

    def sql4joinFirstFilter(self, execute=True):
        q = \
        '''CREATE TABLE %s AS (
            SELECT cur.ride AS c_ride, matched.ride AS mride, cur.geom AS cpath, cur.pickup_geom AS cploc,
             cur.dropoff_geom AS cdloc, 
            cur.trip_distance AS cdist, cur.trip_time_in_secs AS ctime, cur.total_amount AS cfare,
            matched.geom AS mpath, matched.pickup_geom AS mploc, matched.dropoff_geom AS mdloc, 
            matched.trip_distance AS mdist, matched.trip_time_in_secs AS mtime, matched.total_amount AS mfare    
            FROM %s AS ff
            JOIN %s AS cur
            ON cur.ride = ff.cur_ride
            JOIN %s AS matched
            ON matched.ride = ff.ride);
        ''' % (self.params_tab, self.first_filter_tab, self.all_tab, self.all_tab)
        q2 = '''CREATE UNIQUE INDEX ride_ind ON %s (c_ride, mride);''' % self.params_tab
        self.executeQ(q, execute)
        return self.executeQ(q2, execute)
#Filter2
# create table filter_by_p AS (
#     select c_ride, mride,
#      ST_Distance(cploc::geography, mploc::geography) AS pdist
#      from rs_params where ST_Distance(cploc::geography, mploc::geography) <= 200
#      );

#Filter3
#create table filter_by_path_diff AS (
    # select c_ride, mride, pdist, ST_distance(cpath::geography, mpath::geography)
    # from filter_by_p
    # );
#create unique index keyride on filter_by_path_diff (c_ride, mride)

    def sql4evalOSRM(self, execute=True):
        print 'Creating Table for evaluation OSRM query...'
        q = \
        '''CREATE TABLE evaluation_osrm AS (
        SELECT pdf.c_ride, pdf.mride,  
        concat_ws(',', ST_Y(cploc), ST_X(cploc)) AS cur_p,
        concat_ws(',', ST_Y(cdloc), ST_X(cdloc)) AS cur_d,
        concat_ws(',', ST_Y(mploc), ST_X(mploc)) AS mat_p,
        concat_ws(',', ST_Y(mdloc), ST_X(mdloc)) AS mat_d
        FROM filter_by_path_diff AS pdf
        LEFT OUTER JOIN rs_params  AS rp
        ON rp.c_ride = pdf.c_ride AND rp.mride = pdf.mride) ;'''
        q2 = '''CREATE UNIQUE INDEX keyride2 on evaluation_osrm (c_ride, mride)'''
        self.executeQ(q, execute=True)
        return self.executeQ(q2, execute=True)

    def sql4joiningEval(self, execute=True):
        q = \
        '''CREATE TABLE %s AS (
        SELECT e.*, ST_AsText(e.geom),
        a1.osrm_time AS ctime, a1.osrm_dist AS cdist, ST_AsText(a1.geom) AS cpath, a1.osrm_start AS cploc,
        a1.osrm_end AS cdloc, a1.pickup_datetime AS cptime, a1.dropoff_datetime AS cdtime, 
        a2.osrm_time AS mtime, a2.osrm_dist AS mdist, ST_AsText(a2.geom) AS mpath, a2.osrm_start AS mploc,
        a2.osrm_end AS mdloc, a2.pickup_datetime AS mptime, a2.dropoff_datetime AS mdtime,
        a1.pcity AS cpcity, a1.dcity AS cdcity, a1.pname AS cpname, a1.dname AS cdname,
        a2.pcity AS mpcity, a2.dcity AS mdcity, a2.pname AS mpname, a2.dname AS mdname
        FROM %s AS e
        LEFT OUTER JOIN %s AS a1
        ON e.c_ride = a1.ride
        LEFT OUTER JOIN %s AS a2
        ON e.mride = a2.ride);
        ''' % ('final_pred_rs2', self.evaluation_tab, self.all_tab, self.all_tab)
        return self.executeQ(q, execute)

    # def sql4rsFeasibility(self):
    #     q = \
    #     '''CREATE TABLE final_calc_rs AS (
    #         SELECT ((osrm_dist::FLOAT / 1608) - cdist - mdist) AS dist_diff,
    #         (osrm_time - ctime - dtime) AS time_diff,
    #         dist_diff::FLOAT / mdist AS mdist_percent,  
    #         dist_diff::FLOAT / cdist AS cdist_percent,
    #         m


    #         )
    #     '''


    def sql4createTable(self, table_name, name_type, indexes, execute=True):
        '''
        Create table give names
        '''
        template = \
        '''
        CREATE TABLE %s (
            %s, 
            PRIMARY KEY (%s)
        )
        '''
        name_type_lst = [' '. join(name_type[i:i + 2]) for i in range(0, len(name_type), 2)]
        content = ', \n\t\t'.join(name_type_lst)
        indexes_str = ','. join(indexes)         
        q = template % (table_name, content, indexes_str)
        return executeQ(q, execute)

    def copyToTable(self, table_name, fname, cursor, conn):
        
        print 'Copying csv into %s...' % table_name
        q = """COPY %s FROM STDIN WITH
                CSV
                HEADER
                DELIMITER AS ','
            """
        cursor.copy_expert(sql=q % table_name, file=open(fname))
        conn.commit()
     
        