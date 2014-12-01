from sql_queries import SqlQueries


class MatchDistrict(SqlQueries):
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

    def __init__(self, district_fpath, district_fname):
        super(MatchDistrict, self).__init__()
        self.district_fpath = district_fpath
        self.district_fname = district_fname

    def load_geojson_to_db(self, drop_original):
        """INPUT:
        - drop_original(BOOL) [Drop the original table when loading script]

        OUTPUT:
        - NONE

        DOC:
        - Load district geojson into db after converting to shp then sql"""

        self.convert_geojson_to_sql(self.district_fpath, self.district_fname,
                                    self.t_district, single_geom=False)
        self.load_sql_script(drop_original)

    def match_district_to_rides(self):
        """INPUT:
        - NONE

        OUTPUT:
        - NONE

        DOC:
        - Making a table with district info for pickup and dropoff locs"""

        self.sql_rides_district()
