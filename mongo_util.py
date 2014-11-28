from pymongo import MongoClient


class MongoUtil(object):
    """INPUT:
    - db_name(STR) [The name of the database, existing or not]
    - table_name(STR) [The name of the table, existing or not]

    OUTPUT:
    - NONE

    DOC:
    - Abstraction of MongoDB to easily access MongoDB functions"""

    def __init__(self, db_name, table_name):
        self.db_name = db_name
        self.table_name = table_name
        print 'Launching Mongo...'
        self.client = MongoClient()
        print 'DATABASE AND TABLE', self.db_name, self.table_name
        self.mongodb = self.client[self.db_name]
        self.tab = self.mongodb[self.table_name]

    def get_all(self, limit=None):
        """INPUT:
        - limit(INT) [How many entries]

        OUTPUT:
        - (LIST OF DICT, INT) [All the entries in the table, entry count]"""

        if not limit:
            results = self.tab.find()
            count = self.tab.count()
        else:
            results = self.tab.find().limit(limit)
            count = limit
        return (results, count)
