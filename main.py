import bs4
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging
import os
import io
from http import ClientSession
import pandas as pd
from datetime import datetime, date
import asyncio


# Logging:
logger = logging.getLogger(__name__)
logging.getLogger().setLevel(logging.INFO)


# Hardcoded test inputs
START_DATE = '20150617'
END_DATE = '20180630'


# Base url for the initial date search & CSV downlaod:
base_url = 'https://www.cboe.com/us/equities/market_statistics/short_interest'
csv_base_url = 'https://www.cboe.com'


# Get the environment variables for the query period:
date_format = '%Y%m%d'

try:
    START_DATE = datetime.strptime(
        os.environ['START_DATE'], date_format).date()
except (KeyError, ValueError):
    START_DATE = date(2021, 3, 1)
logging.info(" START_DATE: " + str(START_DATE))

try:
    END_DATE = datetime.strptime(os.environ['END_DATE'], date_format).date()
except (KeyError, ValueError):
    END_DATE = date.today()
logging.info(" END_DATE: " + str(END_DATE))


# Create an empty DataFrame to hold the results:
# df = pd.DataFrame(
#     columns=[
#         'data_date',
#         'cycle_settlement_date',
#         'bats_symbol',
#         'security_name',
#         'num_shares_net_short_current_cycle',
#         'num_shares_net_short_previous_cycle',
#         'cycle_avg_daily_trade_vol',
#         'min_num_trade_days_to_cover_short',
#         'split_indicator',
#         'manual_revision_indicator'
#     ])


# Array to hold each CSV's data as a separate dataframe:
dataframes_list = []


async def get_results_page(number_of_months, url_suffixes):
    page_number = 0
    # print(url_suffixes[page_number])

    # Fetch the HTML for each results page:
    while page_number < number_of_months + 1:

        # Response object:
        page = requests.get(
            base_url + url_suffixes[page_number],
            headers={'User-Agent': 'Mozilla/5.0'}
        )

        # Here using lxml as it's beter performance than bs4's bundled parser:
        soup = BeautifulSoup(page.text, "lxml")

        # Pass the soup object to our processing helper function:
        await process_page(soup)

        page_number += 1



async def process_page(soup):

    #  Get all the tags which contain a link (a href) and have a text field ending with ".csv":
    csv_stubs = [a['href'] for a in soup.find_all('a', href=True) if (a.text[-4:] == '.csv')]
    # print(csv_tags)

    # Create a requests Session & Pass all the found csv_tags as a queue for downloading and processing:
    async with ClientSession(trust_env=True) as session:
        await asyncio.gather(*[process_csv(csv_stub, session) for csv_stub in csv_stubs])



async def process_csv(csv_stub, session):

    # Trigger the download:
    csv_bytes, csv_id, = await download_csv(csv_stub, session)

    # Extract the date string from the csv_id:
    csv_data_date = csv_id[-15:-7]
    logging.info(f' CSV Timestamp: {csv_data_date}\n')

    # Format the bytes wth UTF-8:
    decoded_csv = str(csv_bytes,'utf-8')

    # Reconstruct the csv object from the byte stream:
    csv_data = io.StringIO(decoded_csv)

    # Create a new dataframe from the csv:
    df = pd.read_csv(csv_data)

    # Insert the 'data_date' column:
    df.insert(0, 'data_date', csv_data_date )

    # Append the new dataframe to the master dataframes list:
    global dataframes_list
    dataframes_list.append(df)

    # print(df.head())

    

async def download_csv(csv_stub, session):
    csv_size = 0

    try:
        # Create an identifier for the CSV from the url stub:
        csv_id = csv_stub.split('/')[-1]
        
        # Use the existing session object to download the CSV as byte stream:
        url = csv_base_url + csv_stub
        logging.info(f" About to download CSV: {csv_id}")

        csv_response = await session.request(
            method='GET',
            url=url,
            headers={'User-Agent': 'Mozilla/5.0'},
            allow_redirects=True)

        csv_response.raise_for_status()

        if csv_response.status == 200:
            await csv_response.read()
            logging.info(f' Successfully retrieved: {csv_id}')

            # Return the CSV response object:
            return await csv_response.read(), csv_id

    except Exception as e:
        logging.warn(f" Exception while getting the CSV: {str(e)}")

    return None, csv_size



## Creates a single URL suffix from a (YYYY, MM) tuple
def get_date_query_suffix(tuple):
    year, month = tuple
    return "/?year="+year+"&month="+month+"&mkt=bzx"



## Creates a list of URL suffixes
def create_date_suffix_list(date_list):
    return_list = list()

    for x in date_list:
        return_list.append(get_date_query_suffix(x))

    return return_list



if __name__ == "__main__":

    # Use the difference between datetime objects to get the number of months between two dates
    number_of_months = (END_DATE.year - START_DATE.year) * \
        12 + (END_DATE.month - START_DATE.month)

    logging.info(f" Number of months: {str(number_of_months)}")


    # Get the initial date tuple & initialize the date tuple array:
    intial_date_tuple = (
        datetime.strftime(START_DATE, '%Y'),
        datetime.strftime(START_DATE, '%m')
    )
    date_tuples = [intial_date_tuple]


    # Iterate the remaining months and increment the START_DATE by one month each time:
    for i in range(number_of_months + 1):

        if i > 0:
            THIS_DATE = START_DATE + relativedelta(months=i)
            this_date_tuple = (
                datetime.strftime(THIS_DATE, '%Y'),
                datetime.strftime(THIS_DATE, '%m')
            )
            date_tuples.append(this_date_tuple)
    logging.info(date_tuples)


    # Convert the date tuples to url suffixes:
    url_suffixes = create_date_suffix_list(date_tuples)
    logging.info(url_suffixes)


    # Get the async loop to start pulling data:
    loop = asyncio.get_event_loop()
    logging.info(" Got the event loop!")

    try:
        loop.run_until_complete(get_results_page(number_of_months, url_suffixes))
    except Exception as e:
        logging.warning(str(e))


    # Concatenate the list of dataframes:
    final_df = pd.concat(dataframes_list, axis=0, ignore_index=True)


    logging.info(" Completed fetching results...\n")

    logging.info(f' Final Dataframe Size: {final_df.size}\n')
    logging.info(f' Final Dataframe HEAD:\n {final_df.head()}\n')
    logging.info(f' Final Dataframe TAIL:\n {final_df.tail()}\n')
