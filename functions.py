import requests
import pandas as pd
import time
from datetime import date, timedelta
from bs4 import BeautifulSoup
from sql_queries import create_table_queries, drop_table_queries, copy_table_queries,\
     insert_table_queries, truncate_table_queries, update_table_queries

def drop_tables(cur, conn):
    """
    drops tables if exists
    """
    print("Droping tables")
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    creates tables
    """
    print("Creating tables")
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def load_staging_table(cur, conn):
    """
    runs queries to copy data from s3
    """
    print("Loading staging table")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    - runs queries to copy data from staging table to final tables
    """
    print("Inserting data into final tables")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def truncate_tables(cur, conn):
    """
    run queries to truncate staging table
    """
    print("Truncating staging table")
    for query in truncate_table_queries:
        cur.execute(query)
        conn.commit()

def update_tables(cur, conn):
    """
    - runs queries to copy data from staging table to final tables
    """
    print("Updating final tables")
    for query in update_table_queries:
        cur.execute(query)
        conn.commit()

def scrape_website(cur, conn):
    """
    -scrapes website
    -inserts data into staging table
    """
    print("Scraping webiste")

    # timescale
    start_date = date(2021, 10, 1)
    delta = timedelta(days=1)
    end_date = date.today() - delta

    # dataframe for data
    data = pd.DataFrame(columns=['Country', 'Competition', 'Time', 'Home', 'Away', 'Date'])

    # for scraping
    session_requests = requests.session()

    #loop through dates in timescale
    while start_date <= end_date:
        print(start_date)
        time.sleep(5) # give some time so we dont get blocked

        # convert date to strings
        day = start_date.strftime("%d")
        month = start_date.strftime("%m")
        year = start_date.strftime("%Y")

        # website to scrape
        URL = 'https://www.betexplorer.com/results/soccer/?year={}&month={}&day={}'.format(year, month, day)

        # send request
        result = session_requests.get(
            URL, 
            headers = dict(referer = URL)
        )

        # if error let know
        if result.status_code != 200:
            print("Something went wrong for " + start_date)
            print("Status code " + result.status_code)
        
        # scrape
        soup = BeautifulSoup(result.content, "html.parser")

        # get data
        for tbody in soup.body.find_all("tbody"):

            country = tbody.a.get_text().split(": ")[0]
            property = tbody.a.get_text().split(": ")[1]
    
            for tr in tbody.find_all("tr")[1:]:

                
                string_to_search = "data-dt=\"{},{},{}".format(day.lstrip("0"), month.lstrip("0"), year)
                # extra condition to get only todays events
                if "js-newdate" not in str(tr) and string_to_search in str(tr):

                    starttime = tr.span.get_text()
                    home = tr.a.get_text().replace("<strong>", "").replace("</strong>", "").split(" - ")[0]
                    away = tr.a.get_text().replace("<strong>", "").replace("</strong>", "").split(" - ")[1]

                    data.loc[len(data)] = [country, property, starttime, home, away, year + month + day]

        start_date += delta

    # insert scraped data into staging table
    print("Inserting data into staging table")
    # save dataframe as numpy array
    np_data = data.to_numpy() 
    # get string that goes into the query
    args_str = b','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s)", x) for x in tuple(map(tuple,np_data)))
    # execute query
    cur.execute("INSERT INTO staging_events(Country, Competition, Time, Home, Away, Date) VALUES "+args_str.decode("utf-8"))
    conn.commit()

    return data