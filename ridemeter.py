import sys
from db_interface import PsqlInterface
from preprocessing import Preprocessing
from match_district import MatchDistrict
from mongo_util import MongoUtil
from query_osrm import QueryOSRM
from process_store_osrm import ProcessStoreOSRM


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
    route_fname = 'route_two_pt.json'

    # What time period of taxi data do we want to process
    month_of_2013 = 11
    range_of_date = [4, 4]
    range_of_hour = [8, 9]

    # Mongo Table name
    mongo_route_table = 'route_two_pt'

    if 1 in run_lst or 0 in run_lst:
        # Create the database
        PsqlInterface(database_name, created=False,
                      postgis=True, user='postgres')

    if 2 in run_lst or 0 in run_lst:
        # Clean and put raw data into DB
        preprocess_inst = Preprocessing(month=month_of_2013,
                                        day_rng=range_of_date,
                                        hour_rng=range_of_hour)
        preprocess_inst.read_df()
        preprocess_inst.join_df()
        preprocess_inst.clean_df()
        preprocess_inst.create_table_trip_fare()

    if 3 in run_lst or 0 in run_lst:
        # Match district to pickup and dropoff locations
        district_inst = MatchDistrict(district_fpath, district_fname)
        district_inst.load_geojson_to_db(True)  # drop_original
        district_inst.match_district_to_rides()  # Create ride + district table

    if 4 in run_lst or 0 in run_lst:
        # Launch Mongo instance (Wipe Mongo Table)
        mongo_inst_a = MongoUtil(database_name, mongo_route_table, True)
        mongo_tab_a = mongo_inst_a.tab

        # Querying OSRM and push results into Mongo
        osrm_q = QueryOSRM(mongo_tab_a, launch=False)
        osrm_q.run_two_locations()

    if 5 in run_lst or 0 in run_lst:
        # Launch Mongo instance (Keep Mongo Table)
        mongo_inst_b = MongoUtil(database_name, mongo_route_table, False)

        # Pull results from Mongo, decode and push into psql
        osrm_result_cnt_two_pt = mongo_inst_b.get_all()
        osrm_store = ProcessStoreOSRM(osrm_result_cnt_two_pt,
                                      route_fpath,
                                      route_fname)
        osrm_store.make_geojson()
        osrm_store.load_geojson_to_db(True)  # drop_original

    if 6 in run_lst or 0 in run_lst:
        ### filters
        pass


if __name__ == '__main__':
    main()
