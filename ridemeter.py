import sys
from db_interface import PsqlInterface
from preprocessing import Preprocessing
from match_district import MatchDistrict
from mongo_util import MongoUtil


def main():
    """This function rebuilds the whole project from raw data
    By putting a comma separated string as the command line
    argument, you can select which stages to run.
    The argument 0 runs all the stages"""

    run_lst = map(int, sys.argv[1].split(','))

    if 1 in run_lst or 0 in run_lst:
        # Create the database
        PsqlInterface('ridemeter', created=False,
                      postgis=True, user='postgres')

    if 2 in run_lst or 0 in run_lst:
        # Clean and put raw data into DB
        mnt = 11
        d_rng = [4, 4]
        h_rng = [8, 9]
        preprocess_inst = Preprocessing(month=mnt,
                                        day_rng=d_rng,
                                        hour_rng=h_rng)
        preprocess_inst.read_df()
        preprocess_inst.join_df()
        preprocess_inst.clean_df()
        preprocess_inst.create_table_trip_fare()

    if 3 in run_lst or 0 in run_lst:
        # Match district to pickup and dropoff locations
        district_inst = MatchDistrict('zillow-neighborhoods-nyc-4326.json',
                                      '../data/nyc_neighbor')
        district_inst.load_geojson_to_db()
        district_inst.match_district_to_rides()

    if 4 in run_lst or 0 in run_lst:
        # Launch Mongo instance
        mongo_inst = MongoUtil()
        mongo_tab = mongo_inst.tab

        # Querying OSRM


if __name__ == '__main__':
    main()
