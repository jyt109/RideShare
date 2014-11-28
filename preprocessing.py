from sql_queries import SqlQueries
import pandas as pd
import os
from time import time


class Preprocessing(SqlQueries):
    """INPUT:
    - month(INT) [Month of the taxi rides]
    - day_rng(LIST OF 2, OPT) [From what day to what day in the month]
    - hour_rng(LIST OF 2, OPT) [From nth hour on day 1 to nth hour in last day]

    OUTPUT:
    - NONE

    DOC:
    - Please specify day_rng and hour_rng since processing the whole month
      is heavy work for python
    - Read in a subset of the NYC taxi ride data
    - Cleaning it and put it into the DB"""

    def __init__(self, month=11, day_rng=None, hour_rng=None):
        super(Preprocessing, self).__init__()
        self.month = month
        self.day_rng = day_rng
        self.hour_rng = hour_rng
        self.fare_path = '../data/faredata2013'
        self.trip_path = '../data/tripData2013'
        self.fare_fname = os.path.join(self.fare_path,
                                       'trip_fare_%s.csv' % self.month)
        self.trip_fname = os.path.join(self.trip_path,
                                       'trip_data_%s.csv' % self.month)
        self.clean_fname = os.path.join('../data', 'clean_trip_fare.csv')
        # Variables where functions below will write to
        self.trip_df = pd.DataFrame()
        self.fare_df = pd.DataFrame()
        self.trip_fare_df = pd.DataFrame()
        self.cleaned_df = pd.DataFrame()

    def read_df(self):
        '''
        INPUT:
        - NONE

        OUTPUT:
        - (TUPLE OF 2 DATAFRAMES) [trip and fare dfs]

        DOC:
        - Read in a subset of the NYC taxi ride data
        - Cleaning it and put it into the DB
        '''
        print 'Reading in files ...'
        st_ts = time()

        # Read in file names as specified in init
        trip = pd.read_csv(self.trip_fname)
        fare = pd.read_csv(self.fare_fname)

        # Strip spaces in col names
        fare.columns = [col.strip() for col in fare.columns]
        trip.columns = [col.strip() for col in trip.columns]

        # Convert corresponding columns to datetime
        trip['pickup_datetime'] = pd.to_datetime(trip['pickup_datetime'])
        trip['dropoff_datetime'] = pd.to_datetime(trip['dropoff_datetime'])
        fare['pickup_datetime'] = pd.to_datetime(fare['pickup_datetime'])

        print 'Done in: %d seconds' % (time() - st_ts)

        self.trip_df = trip
        self.fare_df = fare

    def join_df(self):
        '''
        INPUT:
        - trip (PANDAS DATAFRAME) [Trip info from read_df(), init]
        - fare (PANDAS DATAFRAME) [Fare info from read_df(), init]

        OUTPUT:
        - (DATAFRAME) [Joint trip and fare info]

        DOC:
        - Joining the 2 dataframes on common unique keys
        '''
        if self.trip_df.empty or self.fare_df.empty:
            print 'Run read_df first... This will throw an error.'

        print 'Joining trip and fare DataFrames ...'
        st_ts = time()

        # Set pickupdatetime as index on trip df
        self.trip_df.set_index('pickup_datetime', inplace=True, drop=False)

        # Select trips in time window if day and time are specified
        if self.day_rng and self.hour_rng:
            start_time = '2013-%02d-%02d %02d:00:00' %  \
                         (self.month, self.day_rng[0], self.hour_rng[0])
            end_time = '2013-%02d-%02d %02d:00:00' % \
                       (self.month, self.day_rng[1], self.hour_rng[1])
            self.trip_df = self.trip_df.ix[start_time: end_time]
        else:
            print 'Day and hour range not specified. Using the month'

        # Specify the columns to keep in trip and fare dfs
        trip_keep = ['medallion', 'hack_license', 'pickup_datetime',
                     'dropoff_datetime', 'passenger_count',
                     'trip_time_in_secs', 'trip_distance',
                     'pickup_longitude', 'pickup_latitude',
                     'dropoff_longitude', 'dropoff_latitude']

        fare_keep = ['medallion', 'hack_license', 'pickup_datetime',
                     'fare_amount', 'total_amount']
        subtrip = self.trip_df[trip_keep]
        subfare = self.fare_df[fare_keep]

        # Merging the 2 dfs
        merge_on = ['medallion', 'hack_license', 'pickup_datetime']
        trip_fare = pd.merge(subtrip, subfare,
                             left_on=merge_on,
                             right_on=merge_on)

        # Deleting excess columns
        del trip_fare['medallion']
        del trip_fare['hack_license']

        print 'Done in: %d seconds' % (time() - st_ts)

        self.trip_fare_df = trip_fare

    def clean_df(self):
        '''
        INPUT:
        - trip_fare(PANDAS DATAFRAME) [From joining_df(), init]

        OUTPUT:
        - clean_trip_fare(PANDAS DATAFRAME)

        DOC:
        - Clean the data and dump into DB
        '''
        if self.trip_fare_df.empty:
            print 'Run join_df first. This will throw an error.'

        print 'Cleaning ...'
        st_ts = time()

        # Only rides with 1 to 3 passengers
        c_a = self.trip_fare_df['passenger_count'].isin(range(1, 4))
        # Only rides with some distance
        c_b = self.trip_fare_df['trip_distance'] > 0
        # Only rides with some time
        c_c = self.trip_fare_df['trip_time_in_secs'] > 0
        # Only rides within the bounds of NYC
        c_d = (self.trip_fare_df['pickup_latitude'].between(40., 43.)) & \
              (self.trip_fare_df['dropoff_latitude'].between(40., 43.))
        c_e = (self.trip_fare_df['pickup_longitude'].between(-74., -70.)) & \
              (self.trip_fare_df['dropoff_longitude'].between(-74., -70.))

        clean_trip_fare = self.trip_fare_df[c_a & c_b & c_c & c_d & c_e]

        # Give the rides id based on pickup time
        clean_trip_fare.sort('pickup_datetime', inplace=True)
        clean_trip_fare['ride'] = range(1, clean_trip_fare.shape[0] + 1)

        print 'Done in: %d seconds' % (time() - st_ts)

        print 'Writing to CSV...'
        clean_trip_fare.to_csv(self.clean_fname, index=False)

        self.cleaned_df = clean_trip_fare

    def create_table_trip_fare(self):
        '''
        INPUT:
        - NONE

        OUTPUT:
        - NONE

        DOC:
        - Write the ride data from CSV to PostGres DB
        '''
        st_ts = time()

        name_type = '''
                    pickup_datetime   timestamp without time zone
                    dropoff_datetime  timestamp without time zone
                    passenger_count   int
                    trip_time_in_secs int
                    trip_distance     double precision
                    pickup_longitude  double precision
                    pickup_latitude   double precision
                    dropoff_longitude double precision
                    dropoff_latitude  double precision
                    fare_amount       double precision
                    total_amount      double precision
                    ride              bigint
                    '''
        indexes = ['ride', 'pickup_datetime', 'dropoff_datetime']
        # Absolute path of the CSV file
        csv_path = os.path.abspath(self.clean_fname)

        self.sql_create_table(self.t_ride, name_type, indexes)
        self.sql_copy_to_table(self.t_ride, csv_path)

        print 'Done in: %d seconds' % (time() - st_ts)
