from db_interface import PsqlInterface
import pandas.io.sql as pdsql


class SqlQueries(PsqlInterface):
    """INPUT:
    -NONE

    OUTPUT:
    - NONE

    DOC:
    - This class contains all the table names and all sql queries
      for all tasks. It builds on top of the DBInterface for more
      specific purposes of this project"""

    def __init__(self,
                 t_ride='ride',
                 t_district='district_bounds',
                 t_ride_dist='ride_district',
                 t_two_min='two_min_city_filter',
                 t_two_min_pick='two_min_city_pickup',
                 t_200_meters='two_min_city_200m_filter',
                 t_two_pt_route='two_pt_route',
                 t_four_pt_route='four_pt_route',
                 t_ride_route_info='ride_route_info',
                 t_all_route='separate_share_route'):
        super(SqlQueries, self).__init__('ridemeter', created=True)
        self.t_ride = t_ride
        self.t_district = t_district
        self.t_ride_dist = t_ride_dist
        self.t_two_min = t_two_min
        self.t_two_min_pick = t_two_min_pick
        self.t_200_meters = t_200_meters
        self.t_two_pt_route = t_two_pt_route
        self.t_four_pt_route = t_four_pt_route
        self.t_ride_route_info = t_ride_route_info
        self.t_all_route = t_all_route

    def sql_rides_district(self):
        """INPUT:
        - NONE

        OUTPUT:
        - NONE

        DOC:
        - Matching the pickup and dropoff locations to districts
          and create a table"""

        # Drop the joint table if it exist
        query_zero = '''DROP TABLE IF EXISTS %s''' % self.t_ride_dist

        # Make Pickup District table with rides
        query_one = \
        '''CREATE TABLE pickup_district AS (
                SELECT r.*, nb.city AS pcity, nb.name AS pname,
                ST_SetSRID(ST_MakePoint(r.pickup_longitude,
                 r.pickup_latitude),4326)
                AS pickup_geom
                FROM %s AS r
                CROSS JOIN %s AS nb
                WHERE
                ST_Within(ST_SetSRID(ST_MakePoint(r.pickup_longitude,
                 r.pickup_latitude),4326), nb.geom)
           );
        ''' % (self.t_ride, self.t_district)

        # Make Dropoff District table with rides
        query_two = \
        '''CREATE TABLE dropoff_district AS (
                SELECT r.ride, nb.city AS dcity, nb.name AS dname,
                ST_SetSRID(ST_MakePoint(r.dropoff_longitude,
                 r.dropoff_latitude),4326) AS dropoff_geom
                FROM %s AS r
                CROSS JOIN %s AS nb
                WHERE
                ST_Within(ST_SetSRID(ST_MakePoint(r.dropoff_longitude,
                 r.dropoff_latitude),4326), nb.geom)
           );
        ''' % (self.t_ride, self.t_district)

        # Joining the previous two table to make a combined table
        # Some of the ride location matches 2 district
        # We exclude those
        query_three = \
        '''CREATE TABLE %s AS (
               WITH joint_table AS (
                SELECT p.*, d.dcity, d.dname, d.dropoff_geom
                FROM pickup_district AS p
                INNER JOIN dropoff_district AS d
                ON p.ride = d.ride
               )
               SELECT * FROM joint_table
               WHERE ride IN (
                   SELECT ride
                   FROM joint_table
                   GROUP BY ride
                   HAVING COUNT(ride) = 1
               )
           );
        ''' % self.t_ride_dist  # ride_district

        # Dropping the unnecessary tables
        query_four = \
        '''DROP TABLE IF EXISTS pickup_district;
           DROP TABLE IF EXISTS dropoff_district;'''

        # Executing the queries
        self.execute_q(query_zero, msg='Drop joint table if exists...')
        self.execute_q(query_one, msg='Creating Pickup District Table...')
        self.execute_q(query_two, msg='Creating Dropoff District Table...')
        self.execute_q(query_three, msg='Creating Joint Table...')
        self.execute_q(query_four, msg='Dropping first 2 tables...')

    def sql_ride_longer_than_one_min(self):
        """INPUT:
        - NONE

        OUTPUT:
        - (PANDAS DATAFRAME) [Of all the rides more than 1 min]

        DOC:
        - Filtering ride by those that last more than 1 min"""

        query = \
        '''SELECT ride, pickup_datetime, pcity
           FROM %s
           WHERE trip_time_in_secs > 60
           ORDER BY pickup_datetime, dropoff_datetime;
        ''' % self.t_ride_dist

        # Read into pandas dataframe
        print 'Reading in from DB rides more than 1 min ...'
        more_than_one_min_df = pdsql.read_sql(query, self.engine)
        return more_than_one_min_df

    def sql_ride_200_meter(self):
        """INPUT:
        - NONE

        OUTPUT:
        - NONE

        DOC:
        - Create table that filters for pickups within 200m"""

        query_zero = '''DROP TABLE IF EXISTS %s''' % self.t_200_meters

        query_one = \
        '''CREATE TABLE %s AS (
            WITH cur_tab AS (
                SELECT cride, mride,
                pickup_longitude AS cplng,
                pickup_latitude AS cplat
                FROM %s AS two_min_1
                LEFT OUTER JOIN %s AS ride_area
                ON two_min_1.cride = ride_area.ride
            ), match_tab AS (
                SELECT cride, mride,
                pickup_longitude AS mplng,
                pickup_latitude AS mplat
                FROM %s AS two_min_2
                LEFT OUTER JOIN %s AS ride_area
                ON two_min_2.mride = ride_area.ride
            )
            SELECT cur_tab.cride, cur_tab.mride,
            ST_MakePoint(cplng, cplat) AS cpick_geom,
            ST_MakePoint(mplng, mplat) AS mpick_geom
            FROM cur_tab
            LEFT OUTER JOIN match_tab
            ON match_tab.cride = cur_tab.cride
            AND match_tab.mride = cur_tab.mride
        );
        ''' % (self.t_two_min_pick, self.t_two_min, self.t_ride_dist,
               self.t_two_min, self.t_ride_dist)

        query_two = \
        '''CREATE TABLE %s AS (
               SELECT cride, mride, ST_Distance(cpick_geom::GEOGRAPHY,
                                            mpick_geom::GEOGRAPHY) AS pdist
               FROM %s
               WHERE ST_Distance(cpick_geom::GEOGRAPHY,
                                 mpick_geom::GEOGRAPHY) <= 200
           );
        ''' % (self.t_200_meters, self.t_two_min_pick)

        query_three = '''DROP TABLE IF EXISTS %s''' % self.t_two_min_pick

        self.execute_q(query_zero, msg='Drop 200m filter if exists...')
        self.execute_q(query_one, msg='Making table for filter 200m...')
        self.execute_q(query_two, msg='Filtering pickups within 200m...')
        self.execute_q(query_three, msg='Removing unnecessary tables...')

    def sql_osrm_two_lat_long(self):
        """INPUT:
        - NONE

        OUTPUT:
        - GENERATOR(TUPLES OF LAT, LONG AS STRING)

        DOC:
        - Querying the lat and long of pickup and dropoff locations
        - To feed into the OSRM routing API
        - Route between 4 points"""

        # Select pickup and dropoff Lat, Long
        query = \
        """WITH uniq_rides AS (
                SELECT DISTINCT(cride)
                FROM %s
                UNION
                SELECT DISTINCT(mride)
                FROM %s
           ), uniq_ride_info AS (
                SELECT ride, pickup_latitude, pickup_longitude,
                dropoff_latitude, dropoff_longitude
                FROM %s
                WHERE ride IN (SELECT cride FROM uniq_rides)
           )
           SELECT ride, CONCAT_WS(',', pickup_latitude, pickup_longitude),
           CONCAT_WS(',',dropoff_latitude, dropoff_longitude)
           FROM uniq_ride_info;
        """ % (self.t_200_meters, self.t_200_meters, self.t_ride)

        # Fetching from the query
        return self.execute_fetch(query, msg='Fetching two pt OSRM input')

    def sql_osrm_four_lat_long(self):
        """INPUT
        - NONE

        OUTPUT
        - GENERATOR(TUPLES OF LAT, LONG AS STRING)

        DOC:
        - Querying the lat and long of pickup and dropoff locations
        - To feed into the OSRM routing API
        - Route between 4 points"""

        query = \
        """WITH cur_tab AS (
                SELECT mride, cride,
                CONCAT_WS(',', pickup_latitude, pickup_longitude) AS cploc,
                CONCAT_WS(',', dropoff_latitude, dropoff_longitude) AS cdloc
                FROM %s AS filtered_ride
                LEFT OUTER JOIN %s AS ride
                ON filtered_ride.cride = ride.ride
            ), matched_tab AS (
                SELECT mride, cride,
                CONCAT_WS(',', pickup_latitude, pickup_longitude) AS mploc,
                CONCAT_WS(',', dropoff_latitude, dropoff_longitude) AS mdloc
                FROM %s AS filtered_ride
                LEFT OUTER JOIN %s AS ride
                ON filtered_ride.mride = ride.ride
            )
            SELECT cur_tab.cride, cur_tab.mride,
            cploc, cdloc, mploc, mdloc
            FROM cur_tab
            LEFT OUTER JOIN matched_tab
            ON cur_tab.cride = matched_tab.cride
            AND cur_tab.mride = matched_tab.mride
        """ % (self.t_200_meters, self.t_ride,
               self.t_200_meters, self.t_ride)

        return self.execute_fetch(query, msg='Fetching four pt OSRM input')

    def sql_filtered_ride_route_info(self):
        """INPUT:
        - NONE

        OUTPUT:
        - NONE

        DOC:
        - Create table that contains filtered rides and the route info
        - Not the route itself
        - The route itself is not need, but we need the distance
          the time to score the ride share"""

        query_zero = '''DROP TABLE IF EXISTS %s''' % self.t_ride_route_info

        query_one = \
        '''CREATE TABLE %s AS (
                WITH croute_tab AS (
                    SELECT cride, mride, pdist,
                    osrm_start AS cstart,
                    osrm_end AS cend,
                    osrm_dist AS cdist,
                    osrm_time AS ctime
                    FROM %s AS matched
                    LEFT OUTER JOIN %s AS route_2
                    ON matched.cride = route_2.ride
                ), mroute_tab AS (
                    SELECT cride, mride,
                    osrm_start AS mstart,
                    osrm_end AS mend,
                    osrm_dist AS mdist,
                    osrm_time AS mtime
                    FROM %s AS matched
                    LEFT OUTER JOIN %s AS route_2
                    ON matched.mride = route_2.ride
                ), share_tab AS (
                    SELECT route_4.cride, route_4.mride,
                    osrm_dist AS sdist,
                    osrm_time AS stime,
                    rs_type
                    FROM %s AS matched
                    LEFT OUTER JOIN %s AS route_4
                    ON matched.mride = route_4.mride
                    AND matched.cride = route_4.cride
                )
                SELECT ct.cride, ct.mride, ct.pdist,
                cstart, cend, cdist, ctime,
                mstart, mend, mdist, mtime,
                sdist, stime, rs_type
                FROM croute_tab AS ct
                LEFT OUTER JOIN mroute_tab AS mt
                ON ct.cride = mt.cride
                AND ct.mride = mt.mride
                LEFT OUTER JOIN share_tab AS st
                ON ct.cride = st.cride
                AND ct.mride = st.mride
           )
        ''' % (self.t_ride_route_info,
               self.t_200_meters, self.t_two_pt_route,
               self.t_200_meters, self.t_two_pt_route,
               self.t_200_meters, self.t_four_pt_route)

        self.execute_q(query_zero,
                       msg='Dropping OSRM Info Table if exist...')
        self.execute_q(query_one,
                       msg='Creating OSRM Info Table for Scoring...')

    def sql_join_routes(self):
        """INPUT:
        - NONE

        OUTPUT:
        - NONE

        DOC:
        - Create table that contain the separated and shared routes"""

        query_zero = '''DROP TABLE IF EXISTS %s''' % self.t_all_route

        query_one = \
        '''CREATE TABLE %s AS (
                WITH croute_tab AS (
                    SELECT cride, mride,
                    geom AS croute
                    FROM %s AS matched
                    LEFT OUTER JOIN %s AS route_2
                    ON matched.cride = route_2.ride
                ), mroute_tab AS (
                    SELECT cride, mride,
                    geom AS mroute
                    FROM %s AS matched
                    LEFT OUTER JOIN %s AS route_2
                    ON matched.mride = route_2.ride
                ), share_tab AS (
                    SELECT route_4.cride, route_4.mride,
                    geom AS sroute
                    FROM %s AS matched
                    LEFT OUTER JOIN %s AS route_4
                    ON matched.mride = route_4.mride
                    AND matched.cride = route_4.cride
                )
                SELECT ct.cride, ct.mride,
                croute, mroute, sroute
                FROM croute_tab AS ct
                LEFT OUTER JOIN mroute_tab AS mt
                ON ct.cride = mt.cride
                AND ct.mride = mt.mride
                LEFT OUTER JOIN share_tab AS st
                ON ct.cride = st.cride
                AND ct.mride = st.mride
           )
        ''' % (self.t_all_route,
               self.t_200_meters, self.t_two_pt_route,
               self.t_200_meters, self.t_two_pt_route,
               self.t_200_meters, self.t_four_pt_route)

        self.execute_q(query_zero,
                       msg='Dropping all routes Table if exist...')
        self.execute_q(query_one,
                       msg='Creating all routes Table...')


    def sql_get_osrm_info(self):
        """INPUT:
        - NONE

        OUTPUT:
        - NONE

        DOC:
        - Fetch potential rideshare candidates + osrm info for scoring"""

        query = '''SELECT * FROM %s''' % self.t_ride_route_info

        print 'Getting OSRM info from table %s' % self.t_ride_route_info
        osrm_info_df = pdsql.read_sql(query, self.engine)
        return osrm_info_df
