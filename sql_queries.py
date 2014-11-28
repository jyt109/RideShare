from db_interface import PsqlInterface


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
                 t_ride_dist='ride_district'):
        super(SqlQueries, self).__init__('ridemeter', created=True)
        self.t_ride = t_ride
        self.t_district = t_district
        self.t_ride_dist = t_ride_dist

    def sql_rides_district(self):
        """INPUT:
        - NONE

        OUTPUT:
        - NONE

        DOC:
        - Matching the pickup and dropoff locations to districts
          and create a table"""

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
           );''' % (self.t_ride, self.t_district)

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
           );''' % (self.t_ride, self.t_district)

        # Joining the previous two table to make a combined table
        query_three = \
        '''CREATE TABLE %s AS (
            SELECT p.*, d.dcity, d.dname, d.dropoff_geom
            FROM pickup_district AS p
            INNER JOIN dropoff_district AS d
            ON p.ride = d.ride
           );
        ''' % self.t_ride_dist # ride_district

        # Dropping the unnecessary tables
        query_four = \
        '''DROP TABLE pickup_district;
           DROP TABLE dropoff_district;'''

        # Executing the queries
        self.execute_q(query_one, msg='Creating Pickup District Table...')
        self.execute_q(query_two, msg='Creating Dropoff District Table...')
        self.execute_q(query_three, msg='Creating Joint Table...')
        self.execute_q(query_four, msg='Dropping first 2 tables...')

    def sql_osrm_lat_long(self):
        """INPUT:
        - NONE

        OUTPUT:
        - GENERATOR(TUPLES OF LAT, LONG AS STRING)

        DOC:
        - Querying the lat and long of pickup and dropoff locations
        - To feed into the OSRM routing API"""

        # Select pickup and dropoff Lat, Long
        query = \
        '''SELECT ride, CONCAT_WS(',', pickup_latitude, pickup_longitude),
           CONCAT_WS(',',dropoff_latitude, dropoff_longitude)
           FROM %s
        ''' % self.t_ride

        # Fetching from the query
        msg = 'Getting pickup and dropoff coordinates...'
        return self.execute_fetch(query, msg=msg)
