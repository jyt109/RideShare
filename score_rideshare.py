from sql_queries import SqlQueries
import pandas as pd
import os
import json
import shutil


class RideShareScoring(SqlQueries):
    """INPUT:
    - NONE

    OUTPUT:
    - NONE

    DOC:
    - Score potential shared rides in Python
    - Based on Miles Saved and Extra Time Cost"""
    def __init__(self, datatable_json_fpath, datatable_json_fname):
        # Instantiating the superclass SqlQueries
        super(RideShareScoring, self).__init__()
        self.osrm_info_df = pd.DataFrame()
        self.datatable_json_path = os.path.join(datatable_json_fpath,
                                                datatable_json_fname)
        self.get_route_info()

    def get_route_info(self):
        """INPUT:
        - NONE [SQL Query to extract OSRM info]

        OUTPUT:
        - NONE [Pandas DataFrame assigned to instance variable]

        DOC:
        - Pull all OSRM info into a pandas dataframe"""

        print 'Reading in Data to process...'
        self.osrm_info_df = self.sql_get_osrm_info()
        print self.osrm_info_df.head()

    def apply_scoring(self):
        """INPUT:
        - NONE

        OUTPUT:
        - NONE

        DOC:
        - Score potential shared rides in Python
        - Based on Miles Saved and Extra Time Cost"""

        osrm_df = self.osrm_info_df

        #################################################
        ##### Calculate Extra Time Cost to RideShare ####
        #################################################
        # Ranges from +ve of the longer ride (very short shared ride)
        # To bigger than -ve of the longer ride (very long shared ride)
        unshared_time = osrm_df['ctime'] + osrm_df['mtime']
        shared_time = osrm_df['stime']
        time_diff = unshared_time - shared_time

        osrm_df['extra time'] = time_diff

        # The lower percentage of extra time the better
        osrm_df['p. extra time'] = time_diff / shared_time

        # Convert second to minute: second string

        #############################################
        ##### Calculate Miles Saved to RideShare ####
        #############################################
        # Ranges from +ve of the longer ride (very short shared ride)
        # To bigger than -ve of the longer ride (very long shared ride)
        unshared_dist = osrm_df['mdist'] + osrm_df['cdist']
        shared_dist = osrm_df['sdist']
        dist_diff = unshared_dist - shared_dist

        # The higher percentage of distance saved the better the shared ride
        osrm_df['p. mile saved'] = dist_diff / unshared_dist

        # Convert meters to miles
        osrm_df['miles saved'] = dist_diff / 1608.

        # Viable ride sharing is defined as distance save > 0
        viable_shares_df = osrm_df[osrm_df['p. mile saved'] > 0]

        # Calculate the score based on extra time and miles saved
        viable_shares_df['score'] = \
            self.scoring(osrm_df['p. extra time'], osrm_df['p. mile saved'])

        # Translate miles saved to money saved
        viable_shares_df['money saved'] = \
            viable_shares_df['miles saved'].apply(self.dist_to_money)

        # Mark unknown street names
        loc_lst = ['cstart', 'cend', 'mstart', 'mend']
        for col in loc_lst:
            viable_shares_df[col].fillna('(Unknown)', inplace=True)

        # Mark the sequence of points the route passes through
        viable_shares_df['point_1'] = viable_shares_df['cstart']
        viable_shares_df['point_2'] = viable_shares_df['mstart']
        point_3 = []
        point_4 = []
        for ind in viable_shares_df.index:
            row = viable_shares_df.ix[ind]
            if row['rs_type'] == 1:
                point_3.append(row['cend'])
                point_4.append(row['mend'])
            elif row['rs_type'] == 2:
                point_3.append(row['mend'])
                point_4.append(row['cend'])
            else:
                print 'Route points not definely properly!'
                print 'Programming exiting...'
                break

        viable_shares_df['point_3'] = point_3
        viable_shares_df['point_4'] = point_4

        # Set current_ride as index
        viable_shares_df.set_index('cride', inplace=True, drop=False)

        # Select subcolumns and group by current ride
        # In order to select the best matched ride
        cols_to_keep = ['cride', 'mride', 'miles saved',
                        'extra time', 'money saved', 'score',
                        'point_1', 'point_2', 'point_3', 'point_4',
                        'rs_type']
        viable_gps = viable_shares_df[cols_to_keep].groupby('cride')
        result_df = self.get_real_shared_rides(viable_gps, cols_to_keep)
        display_df = self.format_df_for_display(result_df)
        print display_df.head()
        self.to_datatable_json(display_df, self.datatable_json_path)

    def get_real_shared_rides(self, groupby_obj, cols):
        """INPUT:
        - groupby_obj(PANDAS GROUPBY) [Grouped by cride]

        OUTPUT:
        - results_df(PANDAS DATAFRAME) [All true shared rides]

        DOC:
        - Only select the max score shared ride
        - Exclude the ride once it is matched in future point of time"""

        matched_rides = []
        result = []
        skipped = 0
        for ride, matched_df in groupby_obj:
            filtered_df = matched_df[~matched_df['mride'].isin(matched_rides)]
            if filtered_df.shape[0]:
                max_score = filtered_df['score'] == filtered_df['score'].max()
                max_score_row = filtered_df[max_score]
                result.append(max_score_row.values.tolist()[0])
                matched_rides.append(max_score_row['mride'].values[0])
            else:
                skipped += 1
        result_df = pd.DataFrame(result, columns=cols)
        return result_df

    def scoring(self, time_col, dist_col):
        """INPUT:
        - time_col(PANDAS SERIES) [Column of extra time]
        - dist_col(PANDAS SERIES) [Column of distance saved]

        OUTPUT:
        - (PANDAS SERIES) [Score weighted by extra_time and distance saved]

        DOC:
        - Score potential shared rides in Python
        - Based on Miles Saved and Extra Time Cost"""

        # Weight of .5:.5 means 1 mile saved counteracts 1 min extra time
        # Weight of .3:.6 means 1 mile saved counteracts 2.3 mins extra time
        time_w = .3
        dist_w = 1 - time_w

        return (((dist_col * dist_w) - (time_col * time_w)) * 100).round(1)

    def dist_to_money(self, dist):
        """INPUT:
        - dist(FLOAT) [Distance saved in miles]

        OUTPUT:
        - money(FLOAT) [Money saved as a result of distance saved]

        DOC:
        - Covert distance saved to money saved"""

        price_per_mile = 2
        base_fare = 2.5

        return (price_per_mile * dist + base_fare).round(2)

    def format_df_for_display(self, input_df):
        """INPUT:
        - input_df(PANDAS DATAFRAME) [Dataframe to be converted]

        OUTPUT:
        - output_df(PANDAS DATAFRAME) [Dataframe for display]

        DOC:
        - Create dataframe for display on webapp"""

        copied_df = input_df.copy()
        stringify_time = lambda x: '%dm %ds' % ((x / 60) + 2, x % 60)
        copied_df['extra time'] = copied_df['extra time'].apply(stringify_time)

        copied_df['money saved'] = \
            copied_df['money saved'].apply(lambda x: '$%.2f' % x)
        copied_df['miles saved'] = \
            copied_df['miles saved'].apply(lambda x: x.round(1))

        # # Renaming the columns for display purposes
        # copied_df.rename(columns={'extra time': 'Extra Time',
        #                           'money saved': 'Money Saved',
        #                           'miles saved': 'Miles Saved'},
        #                  inplace=True)

        name_replace_lst = [
            ['Adams Street / Brooklyn Bridge Boulevard', 'Adams Street'],
            ['Adam Clayton Powell Jr. Boulevard', 'Adam Clayton'],
            ['Queensboro Bridge (Upper Level],', 'Queensboro Bridge'],
            ['86th Street Transverse Road', '86th Street Trsv.'],
            ['79th Street Transverse Road', '79th Street Trsv.'],
            ['Washington Square South', 'Washington Sq. S.'],
            ['Washington Square North', 'Washington Sq. N.'],
            ['Doris C. Freedman Place', 'Doris C. Freedman'],
            ['Fort Washington Avenue', 'Fort Washington A.'],
            ['72nd Street Transverse', '72nd Street Trsv.'],
            ['Washington Square West', 'Washington Sq. W.'],
            ['Fort Hamilton Parkway', 'Fort Hamilton Pk.']
        ]
        be_replace, to_replace_w = zip(*name_replace_lst)
        copied_df.replace(be_replace, to_replace_w, inplace=True)

        def replace_name(street_name):
            """INPUT:
            - input_df(PANDAS DATAFRAME) [Dataframe to be converted]

            OUTPUT:
            - output_df(PANDAS DATAFRAME) [Dataframe for display]

            DOC:
            - Create dataframe for display on webapp"""

            if len(street_name) > 10:
                return street_name.replace('Street', 'St.') \
                        .replace('Approach', '') \
                        .replace('Parkway', 'Pk.') \
                        .replace('South', 'S.') \
                        .replace('North', 'N.') \
                        .replace('East', 'E.') \
                        .replace('West', 'W.') \
                        .replace('Avenue', 'Ave.') \
                        .replace('Square', 'Sq.') \
                        .replace('Boulevard', 'Bld.') \
                        .replace('Drive', 'D.') \
                        .replace('Place', 'Plc.')
            else:
                return street_name

        pt_col = ['point_1', 'point_2', 'point_3', 'point_4']
        copied_df[pt_col] = copied_df[pt_col].applymap(replace_name)
        return copied_df

    def to_datatable_json(self, input_df, fname):
        """INPUT:
        - input_df(PANDAS DATAFRAME) [Dataframe to be converted]

        OUTPUT:
        - output_df(PANDAS DATAFRAME) [Dataframe for display]

        DOC:
        - Create dataframe for display on webapp"""

        lst_of_lst = input_df.values.tolist()
        data_dict = {'data': lst_of_lst}
        json.dump(data_dict, open('%s' % fname, 'w'))
        shutil.copyfile(fname, os.path.join('webappy/data', fname))
