from psqlSetup import DBRideShare
from stage2 import QueryOSRM
import psycopg2
import pandas.io.sql as pdsql

class Evaluate(QueryOSRM):
    """docstring for Evaluate"""
    def __init__(self):
        super(Evaluate, self).__init__(mong_db='evaluate_rs',
                                        mong_tab='four_points',
                                        geojson_fname='osrm_pred.json',
                                        tab_name='osrm_pred')

    def osrmInput(self):  
        print 'Getting Data for Evaluation...'      
        q = '''SELECT * FROM evaluation_osrm'''
        self.cursor.execute(q)
        rows = self.cursor.fetchall()
        print 'Got the Data...'
        return rows

    def getAndPush(self):
        super(Evaluate, self).osrm2geoson2sql(run=2)

    def joinEvalTable(self):
        print 'Making Evaluation Table...'
        super(Evaluate, self).sql4joiningEval()
        print 'Done...'

    def calculateRsFeasibility(self):
        #pull data into panads
        pass

class Predict(DBRideShare):
    """docstring for Predict"""
    def __init__(self):
        super(Predict, self).__init__()

    def selectOneRide(self, ride):
        #psycopg2 cursor
        q = super(Predict, self).sql4selectParams(ride=ride) #11026
        return pdsql.read_sql(q, self.conn)

    def algorithmAndRank(self):
        df.sort(['alpha','path_diff'], inplace=True)

if __name__ == '__main__':
    e = Evaluate()
    e.getAndPush()
    e.joinEvalTable()

    # pred = Predict()
    # pred.selectOneRide()
