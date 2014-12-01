from pymongo import MongoClient


class MongoUtil(object):
    """INPUT:
    - db_name(STR) [The name of the database, existing or not]
    - table_name(STR) [The name of the table, existing or not]

    OUTPUT:
    - NONE

    DOC:
    - Abstraction of MongoDB to easily access MongoDB functions"""

    def __init__(self, m_db_name, m_table_name, wipe):
        self.m_db_name = m_db_name
        self.m_table_name = m_table_name
        print 'Launching Mongo...'
        self.client = MongoClient()
        print 'MONGO DATABASE: ', self.m_db_name
        print 'MONGO TABLE: ', self.m_table_name
        self.mongodb = self.client[self.m_db_name]
        self.tab = self.mongodb[self.m_table_name]
        if wipe:
            print 'Wiping the current table...'
            # Wipe the table before we start (in case of previous entries)
            self.remove_all()

    def remove_all(self):
        """INPUT:
        - NONE

        OUTPUT:
        - NONE

        DOC:
        - Wipe everything in the database"""

        self.tab.remove({})

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
