# Author: Shawn Wang
# Email: wshawn2020@gmail.com
# Github: https://github.com/wshawn2020

from database import *

# it's for configurating the database, username & password accordingly
config_params = {
    "host"      : "localhost",
    "port"      : "5432",
    "database"  : "demo",
    "user"      : "demo",
    "password"  : "demo"
}

if __name__ == "__main__":
    # initialization of database
    database = DATABASE(config_params)

    # connect to the database
    conn = database.connect_database()

    # initialization of table
    database.table_initialization(conn)

    # load data from gz file
    DATA_FILE = os.path.join(os.path.dirname(os.getcwd()), "data.csv.gz")
    database.load_gz_data(conn, DATA_FILE)

    # calculate percentage change between close price
    database.add_column(conn, "mean_change_pct", "NUMERIC(8,4)")
    database.calc_mean_change_pct(conn)

    # calculate maximum high price for given symbol
    database.add_column(conn, "max_high_price", "NUMERIC(8,4)")
    database.calc_max_high_price(conn, "AIA")
    database.calc_max_high_price(conn, "ALL")

    # calculate minimum low price for given symbol
    database.add_column(conn, "min_low_price", "NUMERIC(8,4)")
    database.calc_min_low_price(conn, "AIA")
    database.calc_min_low_price(conn, "ALL")

    # calculate median volume for given symbol
    database.add_column(conn, "median_volume", "NUMERIC")
    database.calc_median_volume(conn, "AIA")
    database.calc_median_volume(conn, "ALL")

    # export csv file
    database.export_csv(conn, "report.csv")

    # close the connection to database
    database.disconnect_database(conn)
