import pandas as pd
import subprocess
import os
from time import time
from sqlalchemy import create_engine, MetaData
from psqlSetup import DBRideShare
from collections import OrderedDict
import psycopg2

class Processing(DBFunction):
    '''
    PARENT: DBFunction
    CHILD: None

    - Reads in the taxi data from csv into pandas
    - Subset from csv
    - Clean data
    '''
    def __init__(self, month=11, day_rng=None, hour_rng=None):
        super(DBFunction, self).__init__() 
        self.month = month
        self.day_rng = day_rng
        self.hour_rng = hour_rng
        self.csv_path = None 

    def read_dfs(self):
        '''
        INPUT: None
        OUTPUT: (DATAFRAME, DATAFRAME) (fare and trip)

        - Read in from csv to pandas
        '''

        print 'Reading in files ...'
        st_ts = time()
        trip_fname = '../data/tripData2013/trip_data_%s.csv' % self.month
        fare_fname = '../data/faredata2013/trip_fare_%s.csv' % self.month

        trip = pd.read_csv(trip_fname)
        fare = pd.read_csv(fare_fname)

        # strip spaces in col names
        fare.columns = [col.strip() for col in fare.columns]
        trip.columns = [col.strip() for col in trip.columns]

        # convert corresponding columns to datetime
        trip['pickup_datetime'] = pd.to_datetime(trip['pickup_datetime'])
        trip['dropoff_datetime'] = pd.to_datetime(trip['dropoff_datetime'])
        fare['pickup_datetime'] = pd.to_datetime(fare['pickup_datetime'])

        print 'Done in: %d seconds' % (time() - st_ts) 

        return (trip, fare)


    def joiningDF(self, trip, fare):
        '''
        INPUT: DATAFRAME, DATAFRAME
        OUTPUT: DATAFRAME

        - Joining the trip dataframe with fare dataframe
        '''

        print 'Joining trip and fare DataFrames ...'
        st_ts = time()
        trip.set_index('pickup_datetime', inplace=True, drop=False)

        # select rides in time window if day and time are specified
        if self.day_rng and self.hour_rng:
            start_time = '2013-%02d-%02d %02d:00:00' % (self.month, self.day_rng[0], self.hour_rng[0])
            end_time = '2013-%02d-%02d %02d:00:00' % (self.month, self.day_rng[1], self.hour_rng[1])
            trip.sort('pickup_datetime', inplace=True)
            trip = trip.ix[start_time : end_time]
        else:
            'Day and hour range not specified! The whole set is being used and it is big ...'

        trip_keep = ['medallion', 'hack_license', 'pickup_datetime',
                    'dropoff_datetime', 'passenger_count',
                    'trip_time_in_secs', 'trip_distance',
                    'pickup_longitude', 'pickup_latitude',
                    'dropoff_longitude', 'dropoff_latitude']

        fare_keep = ['medallion', 'hack_license', 'pickup_datetime', 'fare_amount', 'total_amount']
        subtrip = trip[trip_keep]
        subfare = fare[fare_keep]

        # merging the 2 dfs
        merge_on = ['medallion', 'hack_license', 'pickup_datetime']
        trip_fare = pd.merge(subtrip, subfare, left_on=merge_on, right_on=merge_on)

        del trip_fare['medallion']
        del trip_fare['hack_license']
        print 'Done in: %d seconds' % (time() - st_ts) 

        return trip_fare

    def cleaning(self, trip_fare, dump=True, fname='clean_trip_fare.csv'):
        '''
        INPUT: DATAFRAME, BOOL(OPTIONAL), STRING(OPTIONAL - filename)
        OUTPUT: DATAFRAME

        - Some basic cleaning to get rid of obviously wrong entries
        '''

        print 'Cleaning ...'
        st_ts = time()

        # conditions: passenger count, positive trip distance, lat/long bound
        c1 = trip_fare.passenger_count.isin(range(1, 4))
        c2 = trip_fare.trip_distance > 0
        c3 = trip_fare.trip_time_in_secs > 0
        c4 = (trip_fare.pickup_latitude.between(40., 43.)) & (trip_fare.dropoff_latitude.between(40., 43.))
        c5 = (trip_fare.pickup_longitude.between(-74., -70.)) & (trip_fare.dropoff_longitude.between(-74., -70.))
        
        clean_trip_fare = trip_fare[c1 & c2 & c3 & c4 & c5]
        clean_trip_fare['ride'] = range(1, clean_trip_fare.shape[0] + 1)

        print 'Done in: %d seconds' % (time() - st_ts)

        # get the path the code is running and write data to file 
        if dump:
            clean_trip_fare.to_csv('../data/%s' % fname, index=False)
            os.chdir('..')
            self.csv_path = os.path.join(os.getcwd(), 'data/%s' % fname)

        return clean_trip_fare

    def writeTripFare2Psql(self, table_name):
        '''
        Writing the data to psql
        Small enough to write using to_sql
        '''
        print 'Writing data to %s ...' % table_name
        st_ts = time()

        name_type = \
        ['pickup_datetime', 'timestamp without time zone',
        'dropoff_datetime', 'timestamp without time zone',
        'passenger_count', 'int',
        'trip_time_in_secs', 'int',
        'trip_distance', 'double precision',
        'pickup_longitude', 'double precision',
        'pickup_latitude', 'double precision',
        'dropoff_longitude', 'double precision',
        'dropoff_latitude', 'double precision',
        'fare_amount', 'double precision',
        'total_amount', 'double precision',
        'ride', 'bigint']
        indexes = ['ride', 'pickup_datetime', 'dropoff_datetime'] 

        create_table_q = super(Csv2psql, self).sql4createTable(table_name, name_type, indexes)
        self.cursor.execute(create_table_q)
        self.conn.commit()
        super(Csv2psql, self).copyToTable(table_name, self.csv_path, self.cursor, self.conn)
        self.conn.close()
        print 'Done in: %d seconds' % (time() - st_ts)


if __name__ == '__main__':
    csv2psql = Csv2psql(month=11, day_rng=[4, 4], hour_rng=[8, 9])
    # Done in: 84 seconds 
    trip, fare = csv2psql.readingDFs()
    # Done in: 64 seconds 
    trip_fare = csv2psql.joiningDF(trip, fare)
    # Done in: 0 seconds 
    clean_trip_fare = csv2psql.cleaning(trip_fare)
    # Done in: 0 seconds 
    csv2psql.writeTripFare2Psql('rides_11_4_8')









        



