import configparser
import psycopg2
from functions import drop_tables, create_tables, load_staging_table, insert_tables,\
     scrape_website, truncate_tables, update_tables, check_missing_dates

if __name__ == "__main__":

    # get config
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # ETL FOR HISTORICAL DATA
    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_staging_table(cur, conn) # insert data from S3 bucket into staging table
    insert_tables(cur, conn) # populate final tables with data from staging table

    # ETL FOR NEW DATA
    truncate_tables(cur, conn) # truncate staging table before pupulating
    scrape_website(cur, conn) # scrape website betexplorer.com for new data and inserting into staging table
    update_tables(cur, conn) # update final tables with data from staging table
    check_missing_dates(conn)
     
    conn.close()