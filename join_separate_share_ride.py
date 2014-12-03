from sql_queries import SqlQueries


class JoinSeparateShareRide(SqlQueries):
    """docstring for JoinSeparateShareRide"""
    def __init__(self):
        super(JoinSeparateShareRide, self).__init__()

    def create_table_osrm_info(self):
        """Create the table with info of separate and share rides"""
        self.sql_filtered_ride_route_info()

    def create_table_all_route(self):
        """Join routes from separate and shared rides"""
        self.sql_join_routes()
