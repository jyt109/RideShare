



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





