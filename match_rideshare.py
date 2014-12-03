from sql_queries import SqlQueries
from datetime import timedelta
from time import time
import os


class RideShareFiltering(SqlQueries):
    """INPUT:
    -NONE

    OUTPUT:
    - NONE

    DOC:
    - This class does primary filter for ride sharing candidates"""

    def __init__(self, two_min_window_csv_fpath, two_min_window_csv_fname):
        # Instatiating the superclass SqlQueries
        super(RideShareFiltering, self).__init__()
        self.two_min_window_path = os.path.join(two_min_window_csv_fpath,
                                                two_min_window_csv_fname)

    def two_min_city_filter(self):
        """INPUT:
        - NONE

        OUTPUT:
        - (PANDAS DATAFRAME) [Trimmed down rideshare candidate]

        DOC:
        - Filter by rides more than 1 min
        - Rides that are in the same district
        - Rides with pickup datetime within 2 minutes of each other"""

        st_ts = time()

        # Open csv to be written to
        if os.path.exists(self.two_min_window_path):
            os.remove(self.two_min_window_path)
        two_min_fhandle = open(self.two_min_window_path, 'w')

        # Read rides from database
        more_than_one_min_df = self.sql_ride_longer_than_one_min()
        more_than_one_min_df.set_index('pickup_datetime',
                                       inplace=True,
                                       drop=False)

        row_count = 0
        print 'Loop through rides to find ones within 2 mins...'
        for ind, index_row in enumerate(more_than_one_min_df.iterrows()):
            # Get ride_id, area name (eg. Manhattan)
            # and pickup-time
            row = index_row[1]
            cur_ride = row['ride']
            cur_city = row['pcity']
            cur_pick_time = row['pickup_datetime']
            cur_pick_time_upper = cur_pick_time + timedelta(minutes=2)

            # Get rides within 2 mins of pickup and label with current ride
            # Must already be sorted by time. Sorted when extracted from sql
            within_two_min_df = more_than_one_min_df.ix[cur_pick_time:
                                                        cur_pick_time_upper]

            # Filter ride within the same area
            # Rationale: Would take more than 2 mins to get to another city
            # Get rid of the same ride matched
            c_a = within_two_min_df['ride'] != cur_ride
            c_b = within_two_min_df['pcity'] == cur_city
            within_same_city_two_min_df = within_two_min_df[c_a & c_b]
            within_same_city_two_min_df['cride'] = cur_ride

            # Take the ride_ids and keep count of the rides matched
            rides_matched = within_same_city_two_min_df[['cride', 'ride']]
            row_count += rides_matched.shape[0]

            # The first one is written to csv with headerm
            if ind == 0:
                print 'Sense check if the first one is working'
                print rides_matched.head()
                rides_matched.to_csv(two_min_fhandle, index=False,
                                     header=False)
            else:
                rides_matched.to_csv(two_min_fhandle, index=False,
                                     header=False, mode='a')

            # Keep count of the rides done
            if ind % 2000 == 0:
                print '%s Rides Filtered...' % ind
                print 'Time Lapsed: %s secs' % int(time() - st_ts)

    def create_table_two_min_filter(self):
        """INPUT:
        - NONE

        OUTPUT:
        - NONE

        DOC:
        - Creating a table for the primary matched ride ids
        - Copy in the csv of matched rides above"""

        st_ts = time()

        field_2_type = \
        """cride integer
           mride integer"""

        primary_ks = ['cride', 'mride']

        self.sql_create_table(self.t_two_min, field_2_type, primary_ks)
        self.sql_copy_to_table(self.t_two_min, self.two_min_window_path)

        print 'Done in: %s secs' % int(time() - st_ts)

    def create_table_200m_filter(self):
        """INPUT:
        - NONE

        OUTPUT:
        - NONE

        DOC:
        - Create table that filters for pickups within 200m"""

        st_ts = time()

        self.sql_ride_200_meter()

        print 'Done in: %s secs' % int(time() - st_ts)
