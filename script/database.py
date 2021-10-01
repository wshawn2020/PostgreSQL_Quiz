# Author: Shawn Wang
# Email: wshawn2020@gmail.com
# Github: https://github.com/wshawn2020

import psycopg2
import pandas as pd
import sys
import os

class DATABASE:
    def __init__(self, config_params):
        print("Start initialization of database")
        self.params = config_params
        print("Finish initialization of database")

    def connect_database(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # connect to the PostgreSQL server
            print("Connect to the PostgreSQL database...")
            conn = psycopg2.connect(**self.params)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Connection fails")
            print("Error: %s" % error)
            sys.exit(1)
        print("Success connect to PostgreSQL database")
        return conn

    def disconnect_database(self, conn):
        conn.close()
        print("Success disconnect to PostgreSQL database")

    def insert_single_row(self, conn, insert_req):
        """ Execute INSERT request for every single row"""
        cursor = conn.cursor()
        try:
            cursor.execute(insert_req)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        cursor.close()

    def add_column(self, conn, column, data_type):
        """ Execute add column request """
        cursor = conn.cursor()
        try:
            print("Start altering column %s" % column)
            cursor.execute("ALTER TABLE demotable DROP COLUMN IF EXISTS %s" % column)
            cursor.execute("ALTER TABLE demotable ADD COLUMN  %s %s;" % (column, data_type))
            conn.commit()
            print("Add column %s successfully" % column)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Add column %s fails" % column)
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        cursor.close()

    def calc_max_high_price(self, conn, symbol):
        """ Calculate maximum high price for given symbol over all data """
        cursor = conn.cursor()
        try:
            print("Start calculating max high price of %s" % symbol)
            cursor.execute("UPDATE demotable "
                           "SET max_high_price = (SELECT MAX(high_price) FROM demotable WHERE symbol='%s')"
                           "WHERE symbol='%s';" % (symbol, symbol))
            conn.commit()
            print("Calculate max high price of %s successfully" % symbol)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Calculate max high price of %s fails" % symbol)
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        cursor.close()

    def calc_min_low_price(self, conn, symbol):
        """ Calculate minimum low price for given symbol over all data """
        cursor = conn.cursor()
        try:
            print("Start calculating min low price of %s" % symbol)
            cursor.execute("UPDATE demotable "
                           "SET min_low_price = (SELECT MIN(low_price) FROM demotable WHERE symbol='%s')"
                           "WHERE symbol='%s';" % (symbol, symbol))
            conn.commit()
            print("Calculate min low price of %s successfully" % symbol)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Calculate min low price of %s fails" % symbol)
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        cursor.close()

    def calc_median_volume(self, conn, symbol):
        """ Calculate median volume for given symbol over all data """
        cursor = conn.cursor()
        try:
            print("Start calculating median volume")
            cursor.execute("UPDATE demotable "
                           "SET median_volume = (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY volume)"
                           "FROM demotable WHERE symbol='%s')"
                           "WHERE symbol='%s';" % (symbol, symbol))
            conn.commit()
            print("Calculate median volume successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Calculate median volume fails")
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        cursor.close()

    def calc_mean_change_pct(self, conn):
        """ Calculate percentage change between close price for given symbol and the preceeding date's value """
        cursor = conn.cursor()
        try:
            print("Start calculating percentage change")
            cursor.execute("CREATE TABLE demotable_tmp AS "
                           "SELECT *,"
                           "LAG(close_price,1) OVER(PARTITION BY symbol ORDER BY date ASC) last_close_price FROM demotable;")
            cursor.execute("UPDATE demotable_tmp "
                           "SET mean_change_pct = - 100.0 * (1 - close_price/ last_close_price);")
            cursor.execute("DROP TABLE IF EXISTS demotable;")
            cursor.execute("ALTER TABLE IF EXISTS demotable_tmp RENAME TO demotable;")
            conn.commit()
            print("Calculate percentage change successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Calculate percentage change fails")
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        cursor.close()

    def export_csv(self, conn, export_name):
        """ Export data interested and be in CSV format """
        cursor = conn.cursor()
        try:
            print("Start exporting csv file")
            # Use the COPY function on the SQL we created above.
            export_command = "COPY " \
                                  "(SELECT date, symbol, mean_change_pct, max_high_price, min_low_price, median_volume " \
                                  "FROM demotable ORDER BY symbol, date ASC)" \
                                  "TO STDOUT WITH CSV HEADER"
            # Set up a variable to store our file path and name.
            export_path = os.path.join(os.path.join(os.path.dirname(os.getcwd()), "export"), export_name)
            os.mkdir(os.path.join(os.path.dirname(os.getcwd()), "export"))

            with open(export_path, 'w') as output:
                cursor.copy_expert(export_command, output)
            conn.commit()
            print("Export csv file successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Export csv file fails")
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        cursor.close()

    def table_initialization(self, conn):
        """ Initialization of table """
        cursor = conn.cursor()
        try:
            print("Start initialization of demotable")
            cursor.execute("DROP TABLE IF EXISTS demotable")
            cursor.execute("CREATE TABLE IF NOT EXISTS demotable "
                           "(date date, symbol varchar(20), name varchar(40), open_price numeric, high_price numeric, "
                           "low_price numeric, close_price numeric, volume numeric);")
            conn.commit()
            print("Initial demotable successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Initial demotable fails")
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return -1
        cursor.close()

    def load_gz_data(self, conn, filepath):
        """ Load gz data into table """
        try:
            print("Start reading gz file")
            df = pd.read_csv(filepath, compression='gzip', header=0, sep=',', quotechar='"')
            print("Read gz file successfully")

            print("Start insert data into database table")
            # Insert each row into table
            for i in df.index:
                query = """
                INSERT INTO demotable(date, symbol, name, open_price, high_price, low_price, close_price, volume) 
                VALUES('%s', '%s', '%s', %f, %f, %f, %f, %d);
                """ % (df['date'][i], df['symbol'][i], df['name'][i], df['open'][i],
                       df['high'][i], df['low'][i], df['close'][i], df['volume'][i])
                self.insert_single_row(conn, query)
            print("Finish insert data into database table")

        except (Exception, psycopg2.DatabaseError) as error:
            print("Load gz file fails")
            print("Error: %s" % error)
            return 1