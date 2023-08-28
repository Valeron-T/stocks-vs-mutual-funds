import configparser  # Used to parse a set of config options such as db connection details
import datetime  # Convert strings to datetime
import decimal  # Convert strings to decimal with fixed precision
import sys  # System tools
import petl  # Python ETL library - provides various functions to perform each step in the ETL process
import psycopg2   # Postgres driver
import requests  # Used to make requests to API endpoints
import requests_cache  # Cache requests

requests_cache.install_cache('data')

# get data from configuration file
config = configparser.ConfigParser()
config.read('config.ini')


def extract(stock_symbol):
    # 3 Types of Mutual Funds Past Performance data
    nifty50_raw = requests.get('https://api.mfapi.in/mf/101525').json()
    mid_cap_raw = requests.get('https://api.mfapi.in/mf/101065').json()
    small_cap_raw = requests.get('https://api.mfapi.in/mf/100177').json()
    
    print(nifty50_raw)

    # Selected stock data - Updated weekly (daily data is paid)
    selected_stock_raw = requests.get(f"https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol={stock_symbol}.BSE&outputsize=full&apikey={config['CONFIG']['alphavantage_api_key']}").json()

    return nifty50_raw['data'], mid_cap_raw['data'], small_cap_raw['data'], selected_stock_raw


def transform(data):
    # FORMAT DATA
    # Format the obtained json into DateTime and fixed precision decimal objects
    nifty50_formatted = [{
        'date': datetime.datetime.strptime(x['date'],'%d-%m-%Y'),
        'nifty50_nav': decimal.Decimal(x['nav']),
    } for x in data[0]]

    mid_cap_formatted = [{
        'date': datetime.datetime.strptime(x['date'], '%d-%m-%Y'),
        'midcap_nav': decimal.Decimal(x['nav']),
    } for x in data[1]]

    small_cap_formatted = [{
        'date': datetime.datetime.strptime(x['date'], '%d-%m-%Y'),
        'smallcap_nav': decimal.Decimal(x['nav']),
    } for x in data[2]]

    selected_stock_formatted = [{
        'date': datetime.datetime.strptime(x, '%Y-%m-%d'),
        'stock_price': decimal.Decimal(y['5. adjusted close']),
    } for x,y in zip(data[3]['Weekly Adjusted Time Series'].keys(), data[3]['Weekly Adjusted Time Series'].values())]

    # print(selected_stock_formatted)

    # Import objects into Petl
    nifty50 = petl.io.json.fromdicts(nifty50_formatted)
    print(nifty50)
    mid_cap = petl.io.json.fromdicts(mid_cap_formatted)
    small_cap = petl.io.json.fromdicts(small_cap_formatted)
    selected_stock = petl.io.json.fromdicts(selected_stock_formatted)

    # Join all nav values based on date
    merged_nav = petl.outerjoin(nifty50, mid_cap, key='date')
    merged_nav = petl.outerjoin(merged_nav, small_cap, key='date')
    merged_nav = petl.outerjoin(merged_nav, selected_stock, key='date')

    # HANDLING MISSING DATA
    # For missing dates, fill nav of previous date
    merged_nav = petl.filldown(merged_nav)

    # Given that the earliest mutual fund data available is from 2006, exclude all values before that date.
    merged_nav = petl.select(merged_nav, lambda rec: rec.nifty50_nav is not None)

    # Visualise how fill down works (Comment fill down and select before printing)
    # print(petl.select(merged_nav, lambda rec: rec.nifty50_nav is None))
    # print(petl.select(merged_nav, lambda rec: rec.date > datetime.datetime.strptime('01-04-2009', '%d-%m-%Y')))

    print(merged_nav)
    return merged_nav
    # print(data[3]['Time Series (Daily)'])


def load(data):
    # intialize database connection
    try:
        dbConnection = psycopg2.connect(host=config['CONFIG']['host'], database=config['CONFIG']['database'], user=config['CONFIG']['user'], password=config['CONFIG']['password'])
    except Exception as e:
        print('could not connect to database:' + str(e))
        sys.exit()

    # populate prices database table
    try:
        petl.io.todb(data, dbConnection, 'prices')
    except Exception as e:
        print('could not write to database:' + str(e))
    print(data)


if __name__ == '__main__':
    # Extraction
    stock_symbol = 'TCS'  # TCS, ITC, VBL (Any BSE listed stock symbol)
    extracted_data = extract(stock_symbol)

    # Good stock examples - RELIANCE, INFY, TCS, ITC
    # Bad stock examples - YESBANK, YAARI
    
    # Transformation
    transformed_data = transform(extracted_data)
    
    # Load
    load(transformed_data)
    