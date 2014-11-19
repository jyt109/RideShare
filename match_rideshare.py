import psycopg2
from psqlSetup import DBRideShare
import pandas as pd 
from datetime import datetime, timedelta
import os

class MatchRideShare(DBRideShare):
    def __init__(self, firstFilter_fname='filter1RideShare.csv'):
        super(RideShare, self).__init__()
        self.firstFilter_fname = firstFilter_fname

    def firstFilter(self):
        '''
        1. Read from psql
        2. Filter by 2 min within pickup time for each trip
        3. Already filter for route of zero distance, so no need here
        '''
        print 'Reading in Data...'
        df = super(RideShare, self).sql4RSFilter1()
        df.set_index('pickup_datetime', inplace=True, drop=False)

        f = open('../data/filters/' + self.firstFilter_fname, 'w')
        cnt = 0
        print 'Loop through rides to find ones within 2 mins...'
        for ind, row in enumerate(df.iterrows()):
            s = row[1]
            cur_ride = s.ride
            cur_city = s.pcity
            dfByTime = df.ix[s.pickup_datetime : (s.pickup_datetime + timedelta(minutes=2))]
            dfByTime['cur_ride'] = cur_ride
            dfByTimeByDistrict = dfByTime[(dfByTime.ride != dfByTime.cur_ride) & (dfByTime.pcity == cur_city)]
            ready = dfByTimeByDistrict[['ride', 'cur_ride']]
            cnt += ready.shape[0]
            if cur_ride == 1 or cur_ride % 2000 == 0: 
                print 'Done ', cur_ride
                print ready.head()
            if ind == 0:
                ready.to_csv(f, index=False)
            else:
                ready.to_csv(f, index=False, header=False, mode='a')
        print 'There are in total %d Ride Share candidates for the first filter' % cnt
        f.close()

    def writeFirstFilterToDB(self):
        name_type = \
        ['ride', 'bigint',
        'cur_ride', 'bigint']
        indexes = ['ride', 'cur_ride'] 
        os.chdir('../data/filters')
        create_table_q = super(RideShare, self).sql4createTable(self.first_filter_tab, name_type, indexes)
        super(RideShare, self).copyToTable(self.first_filter_tab, self.firstFilter_fname, self.cursor, self.conn)

    def writeSubFilter(self):
        super(RideShare, self).sql4subFilter()

    def storeRSParams(self):
        print 'Joining ride share tables...'
        return super(RideShare, self).sql4joinFirstFilter()

    def storeFilteredRides(self):
        print 'Storing filtered rides...'
        return super(RideShare, self).sql4getFilteredRides()


if __name__ == '__main__':
    rs = MatchRideShare()
    rs.firstFilter()
    rs.writeFirstFilterToDB()
    rs.storeRSParams()
    rs.storeFilteredRides()
    rs.writeSubFilter()
    