import sys
from db_interface import PsqlInterface
from preprocessing import Preprocessing
from match_district import MatchDistrict
from mongo_util import MongoUtil
from query_osrm import QueryOSRM
from process_store_osrm import ProcessStoreOSRM
from match_rideshare import RideShareFiltering
from join_separate_share_ride import JoinSeparateShareRide
from score_rideshare import RideShareScoring


def main():
    """This function rebuilds the whole project from raw data
    By putting a comma separated string as the command line
    argument, you can select which stages to run.
    The argument 0 runs all the stages"""

    # Specifies the stages that have to run, e.g 1,2,3,5
    run_lst = [int(stage.strip()) for stage in sys.argv[1].split(',')]

    # Settings to run the project
    # Both PostGres and Mongo DB name
    database_name = 'ridemeter'

    # Location of the NYC district bound geojson file
    district_fpath = '../data/nyc_neighbor'
    district_fname = 'zillow-neighborhoods-nyc-4326.json'

    # Location of the route geojson file
    route_fpath = '../data/route'
    route_fname = 'route.json'

    # Location of 2 min window csv
    two_min_window_fpath = '../data/filter'
    two_min_window_fname = 'two_min_window.csv'

    # Location display csv
    display_csv_fpath = '../data'
    display_csv_fname = 'display.csv'

    # What time period of taxi data do we want to process
    month_of_2013 = 11
    range_of_date = [4, 4]
    range_of_hour = [8, 9]

    # Mongo Table name
    two_pt_route_table = 'route_two_pt'
    four_pt_route_table = 'route_four_pt'

    ###############################
    ###    1. CREATE DATABASE   ###
    ###############################
    if 1 in run_lst or 0 in run_lst:
        # Create the database
        PsqlInterface(database_name, created=False,
                      postgis=True, user='postgres')

    ###############################
    ###    2. PREPROCESS DATA   ###
    ###############################
    if 2 in run_lst or 0 in run_lst:
        # Clean and put raw data into DB
        preprocess_inst = Preprocessing(month=month_of_2013,
                                        day_rng=range_of_date,
                                        hour_rng=range_of_hour)
        preprocess_inst.read_df()
        preprocess_inst.join_df()
        preprocess_inst.clean_df()
        preprocess_inst.create_table_trip_fare()

    ######################################
    ###    3. MATCH DISTRICT TO RIDES  ###
    ######################################
    if 3 in run_lst or 0 in run_lst:
        # Match district to pickup and dropoff locations
        district_inst = MatchDistrict(district_fpath, district_fname)
        district_inst.load_geojson_to_db(True)  # drop_original
        district_inst.match_district_to_rides()  # Create ride + district table

    #########################################################
    ###    4 / 5. FILTER BY 2 MINS PICKUP WINDOW / 200M   ###
    #########################################################
    if 4 in run_lst or 5 in run_lst or 0 in run_lst:
        rs_filter = RideShareFiltering(two_min_window_fpath,
                                       two_min_window_fname)
        if 4 in run_lst or 0 in run_lst:
            # Filtering for rides two minutes within
            #(pickup time) and same city
            rs_filter.two_min_city_filter()
            rs_filter.create_table_two_min_filter()

        if 5 in run_lst or 0 in run_lst:
            #Filtering for rides within 200m
            rs_filter.create_table_200m_filter()

    #################################################
    ###    6. GET SHARED PATH FOR FILTERED RIDES  ###
    #################################################
    if 6 in run_lst or 0 in run_lst:
        # Launch Mongo instance (Wipe Mongo Table)
        mongo_inst_a = MongoUtil(database_name, four_pt_route_table, True)
        mongo_tab_a = mongo_inst_a.tab

        # Querying OSRM and push results into Mongo
        osrm_q = QueryOSRM(mongo_tab_a, launch=True)
        osrm_q.run_four_locations()

    ##########################################################
    ###    7. STORE SHARED PATHS FOR FILTERED RIDES (PSQL) ###
    ##########################################################
    if 7 in run_lst or 0 in run_lst:
        # Launch Mongo instance (Keep Mongo Table)
        mongo_inst_b = MongoUtil(database_name, four_pt_route_table, False)

        # Pull results from Mongo, decode and push into psql
        osrm_result_cnt_four_pt = mongo_inst_b.get_all()
        osrm_store = ProcessStoreOSRM(osrm_result_cnt_four_pt,
                                      'four_pt',
                                      route_fpath,
                                      route_fname)
        osrm_store.make_geojson()
        osrm_store.load_geojson_to_db(True)  # drop_original

    ####################################################
    ###    8. GET SEPARATE PATHS FOR FILTERED RIDES  ###
    ####################################################
    if 8 in run_lst or 0 in run_lst:
        # Launch Mongo instance (Wipe Mongo Table)
        mongo_inst_a = MongoUtil(database_name, two_pt_route_table, True)
        mongo_tab_a = mongo_inst_a.tab

        # Querying OSRM and push results into Mongo
        osrm_q = QueryOSRM(mongo_tab_a, launch=True)
        osrm_q.run_two_locations()

    #############################################################
    ###    9. STORE SEPARATE PATHS FOR FILTERED RIDES (PSQL)  ###
    #############################################################
    if 9 in run_lst or 0 in run_lst:
        # Launch Mongo instance (Keep Mongo Table)
        mongo_inst_b = MongoUtil(database_name, two_pt_route_table, False)

        # Pull results from Mongo, decode and push into psql
        osrm_result_cnt_two_pt = mongo_inst_b.get_all()
        osrm_store = ProcessStoreOSRM(osrm_result_cnt_two_pt,
                                      'two_pt',
                                      route_fpath,
                                      route_fname)
        osrm_store.make_geojson()
        osrm_store.load_geojson_to_db(True)  # drop_original

    ##########################################
    ###    10. MAKE FILE FOR DISPLAY INFO  ###
    ##########################################
    if 10 in run_lst or 0 in run_lst:
        join_inst = JoinSeparateShareRide()
        join_inst.create_table_osrm_info()
        join_inst.create_table_all_route()

    ##########################################
    ###    11. MAKE FILE FOR DISPLAY INFO  ###
    ##########################################
    if 11 in run_lst or 0 in run_lst:
        rscore_inst = RideShareScoring(display_csv_fpath, display_csv_fname)
        rscore_inst.apply_scoring()

if __name__ == '__main__':
    main()
