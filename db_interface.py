import subprocess
import os
import re
import psycopg2
from psycopg2 import ProgrammingError
from sqlalchemy import create_engine


class PsqlInterface(object):
    """INPUT:
    - db_name(STR) [Name of the database]
    - created(BOOL, OPT) [Indicate if the db is created]
    - postgis(BOOL, OPT) [Indicate if POSTGIS is needed]
    - user(STR, OPT) [Username for psql]

    OUTPUT:
    - NONE

    DOC:
    - Database Credentials
    - Create Database and / or POSTGIS extension
    - Contain DB helper functions to avoid interacting
      with psql client directly"""

    def __init__(self,
                 db_name,
                 created=True,
                 postgis=False,
                 user='postgres'):

        # Database Credentials
        self.db_name = db_name
        self.user = user

        # Create the DB if it is not already created
        if not created:
            self.sql_create_db()

        # Define the connections based on created DB
        self.engine = create_engine('postgresql://:@localhost/%s' % db_name)
        self.conn = psycopg2.connect(dbname=self.db_name,
                                     user=self.user,
                                     host='/tmp')
        self.cursor = self.conn.cursor()

        # Create POSTGIS extension if required
        if not created and postgis:
            self.sql_postgis_extension()

        # Path where the code lives
        self.code_path = os.getcwd()

        # SQL file to be loaded in (Filled in by convert_geojson_to_sql)
        self.sql_fname = ''

        # The table name of the SQL file to be loaded in
        self.sql_load_table_name = ''

    def sql_create_db(self):
        """INPUT:
        - execute(BOOL, OPT) [Indicates if query is executed]

        OUTPUT:
        - NONE

        DOC:
        - Create the database that will contain the project"""

        start_conn = psycopg2.connect(dbname='postgres',
                                      user=self.user,
                                      host='/tmp')

        # Need to set isolation_level 0 in order to create database
        start_conn.set_isolation_level(0)
        start_cursor = start_conn.cursor()
        query = 'CREATE DATABASE %s' % self.db_name
        msg = 'Creating Database %s...' % self.db_name
        self.execute_q(query, msg=msg, conn=start_conn, cur=start_cursor)
        start_conn.close()
        start_cursor.close()

    def sql_postgis_extension(self):
        """
        INPUT:
        - execute(BOOL, OPT) [Indicates if query is executed]

        OUTPUT:
        - NONE

        DOC:
        - Create POSTGIS extension if necessary"""

        query = 'CREATE EXTENSION postgis;'
        self.execute_q(query, msg='Creating POSTGIS...')

    def execute_q(self, query, msg=None, conn=None, cur=None):
        """INPUT:
        - q(STR) [The psql query to be executed]
        - msg(STR, OPT) [The msg printed before query is executed]
        - conn(PSYCOPG CONN, OPT) [Default to init, read below]
        - cur(PSYCOPG CURSOR, OPT) [Default to init, read below]

        OUTPUT:
        - NONE

        DOC:
        - Execute psql query
        - Roll back if transaction failed"""

        if not conn:
            conn = self.conn
        if not cur:
            cur = self.cursor
        if msg:
            print msg
        else:
            print 'Executing query...'
        try:
            cur.execute(query)
            conn.commit()
        except ProgrammingError as err_msg:
            print err_msg
            conn.rollback()

    def execute_fetch(self, query, msg=None, conn=None, cur=None):
        """INPUT:
        - q(STR) [The psql query to be executed]
        - msg(STR, OPT) [The msg printed before query is executed]
        - conn(PSYCOPG CONN, OPT) [Default to init]
        - cur(PSYCOPG CURSOR, OPT) [Default to init]

        OUTPUT:
        - (GENERATOR OF TUPLES) [Results from query]

        DOC:
        - Execute psql query
        - Roll back if transaction failed"""

        if not conn:
            conn = self.conn
        if not cur:
            cur = self.cursor
        if msg:
            print msg
        else:
            print 'Executing query...'
        try:
            cur.execute(query)
            return cur.fetchall()
        except ProgrammingError as err_msg:
            print err_msg
            conn.rollback()

    def sql_create_table(self, table_name, field_name2type_str, primary_ks):
        """INPUT:
        - table_name(STR) [The name of the table to be created]
        - field_name2type(STR) [Field name of table and the type]
        - primary_ks(LIST) [List of primary keys to index on]

        OUTPUT:
        - NONE

        DOC:
        - Create table in psql DB given the table specifications
        - Roll back if transaction failed or table already exists"""

        template = '''
        CREATE TABLE %s (
            %s,
            PRIMARY KEY (%s)
        )'''

        field_str = ',\n'.join(field_name2type_str.strip().split('\n'))
        primary_ks_str = ','. join(primary_ks)

        query = template % (table_name, field_str, primary_ks_str)
        msg = 'Creating table %s...' % table_name
        self.execute_q(query, msg=msg)

    def sql_create_index(self, table_name, index_cols):
        """INPUT:
        - table_name(STR) [The name of the table to be indexed]
        - index_cols(STR) [List of col names to be indexed]

        OUTPUT:
        - NONE

        DOC:
        - Create unique index on specified columns"""

        index_str = ','.join(index_cols)
        query = '''CREATE UNIQUE INDEX %s_ind ON %s (%s);''' % \
                (table_name, table_name, index_cols)
        msg = 'Creating Index %s for %s...' % (index_str, table_name)
        self.execute_q(query, msg=msg)

    def sql_copy_to_table(self, table_name, fpath, append=False):
        """INPUT:
        - table_name(STR) [The name of the table to copy to]
        - fpath(STR) [Absolute path of csv file to copy from]

        OUTPUT:
        - NONE

        DOC:
        - Copy CSV file to specified table
        - Roll back if transaction failed"""

        print 'Copying csv into %s...' % table_name
        query = """COPY %s FROM STDIN WITH
                CSV
                HEADER
                DELIMITER AS ','"""

        query_two = """DELETE FROM %s""" % table_name

        try:
            if not append:
                self.execute_q(query_two, msg='Wiping Table before copying..')
            self.cursor.copy_expert(sql=query % table_name, file=open(fpath))
            self.conn.commit()
        except ProgrammingError as err_msg:
            print err_msg
            self.conn.rollback()

    def convert_geojson_to_sql(self, json_path, json_f,
                               table_name, single_geom=True):
        """INPUT:
        - json_f(STR) [Path of json file]
        - table_name(STR) [Name of table to be created]
        - single_geom(BOOL) [True if LineString/ Point; False if Polygon]

        OUTPUT:
        - NONE

        DOC:
        - Convert geojson file into sql file"""

        # Go to the folder with the json file
        os.chdir(json_path)

        # Define the appropriate file names
        fname = re.match(r'(\S+)\.\S+', json_f).group(1)
        shp_f = '%s.shp' % fname
        sql_f = '%s.sql' % fname

        # Use shell command to convert to shape file
        print 'geojson to shp...'
        shp_cmd = 'ogr2ogr -progress -f "ESRI Shapefile" %s %s OGRGeoJSON' % \
                  (shp_f, json_f)
        subprocess.call(shp_cmd, shell=True)

        # Use shell command to convert shape to sql
        print 'shp to sql...'
        # -S give us LineString instead of MultiLineString
        if single_geom:
            subprocess.call('shp2pgsql -I -s 4326 -S %s %s > %s' %
                            (shp_f, table_name, sql_f), shell=True)
        else:
            subprocess.call('shp2pgsql -I -s 4326 %s %s > %s' %
                            (shp_f, table_name, sql_f), shell=True)

        # Remove all unnecessary (not .sql) files
        print 'Removing unnecessary (not .sql) files...'
        for ext in ['.dbf', '.prj', '.shp', '.shx', '.json']:
            if os.path.exists(fname + ext):
                os.remove(fname + ext)

        # Move the sql file to the db file within the code folder
        sql_f_new_path = os.path.join(self.code_path, 'db', sql_f)
        os.rename(sql_f, sql_f_new_path)

        # Return to the code directory
        os.chdir(self.code_path)

        # Assign created sql file as instance variable
        self.sql_fname = sql_f_new_path

        # Assign the table name to instance variable
        self.sql_load_table_name = table_name

    def load_sql_script(self, drop_original, sql_fname=''):
        """INPUT:
        - sql_fname(STR, OPT) [Either provide one or set by instance]

        OUTPUT:
        - NONE

        DOC:
        - Take given sql script and load it into DB"""

        # If no sql file provide, get from instance variable
        # In case we just wanna load in our own sql script
        if not sql_fname:
            sql_fname = self.sql_fname

        if drop_original:
            drop_query = 'DROP TABLE IF EXISTS %s;' % self.sql_load_table_name
            drop_msg = 'Droping Table %s...' % self.sql_load_table_name
            self.execute_q(drop_query, msg=drop_msg)

        query = open(sql_fname).read()
        self.execute_q(query, msg='Loading in %s...' % sql_fname)
