# Kafka Producer to grab stock data from Google finance python api, running via docker

# run example: python kafka_data_ingestion.py CMCSA stock_analyzer 192.168.99.100:9092
# @param $1 - "symbol of the stock", $2 - "kafka topic name", $3 "docker machine ip : kafka port"

from googlefinance_reader import getQuotes
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.errors import KafkaTimeoutError
from time import gmtime, strftime


import argparse
import atexit
import json
import time
import logging
import schedule
import threading

def fetch_price_for_all_symbols(producer, symbols):
    str_time_now = strftime("%Y-%m-%dT%H:%M:%SZ", gmtime())
    threads = []
    for symbol in symbols:
        t = threading.Thread(name='worker %s' % symbol, target=fetch_price, args=(producer, symbol, str_time_now))
        threads.append(t)
        t.start()
        #fetch_price(producer, symbol, str_time_now)

    for t in threads:
        t.join()

def fetch_price(producer, symbol, str_time):
    """
    helper function to get stock data and send to kafka
    @param producer - instance of a kafka producer
    @param symbol - symbol of the stock, string type
    @return None
    """
    logger.debug('Start to fetch stock price for %s', symbol)
    try:
        price = json.dumps(getQuotes(symbol, str_time))
        logger.debug('Get stock info %s', price)
        producer.send(topic = topic_name, value = price, timestamp_ms = time.time())
        logger.debug('Sent stock price for %s to kafka', symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warn('Failed to send stock price for %s to kafka, caused by: %s', (symbol, timeout_error))
    except Exception:
        logger. warn('Fail to get stock price for %s', symbol)

def shutdown_hook(producer):
    try:
        producer.flush(10)
    except KafkaError as KafkaError:
        logger.warn('Failed to flush pending messages to kafka')
    finally:
        try:
            producer.close()
        except Exception as e:
            logger.warn('Fail to close kafka connection')



# setup default parameters
topic_name = 'stock_analyzer'
kafka_broker = '127.0.0.1:9002'
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format = logger_format)
logger = logging.getLogger('data-producer')

logger.setLevel(logging.DEBUG)

# Trace DEBUG INFO WARNING ERROR


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help = 'the symbol of the stock')
    parser.add_argument('topic_name', help = 'the kafka topic')
    parser.add_argument('kafka_broker', help = 'the location of kafka broker')

    # - parse argument
    args = parser.parse_args()
    symbol = args.symbol
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    # initiate a kafka producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        request_timeout_ms=40000
    )
    #symbols = ['MSTR’,'DATA','WMT','AAPL','XOM','MCK','UNH','CVS','GM','T','F','ABC','AMZN','GE','VZ','CAH','COST','WBA','KR','CVX','FNMA','JPM','ESRX','HD','BA','WFC','BAC','GOOGL','MSFT','ANTM','C','CMCSA','IBM','IBM','PSX','JNJ','PG','VLO','TGT','FMCC','LOW','LOW','MET','AET','PEP','ADM','UPS','INTC','PRU','PRU','UTX','MPC','DIS','HUM','PFE','AIG','LMT','SYY','FDX','HPE','CSCO','HPQ','DOW','HCA','KO','KO','CNC','AAL','AAL','MRK','CI','DAL','BBY','HON','CAT','CAT','MS','MS','GS','ETE','ETE','ORCL','TSN','UAL','ALL','ALL','AXP','TJX','NKE','EXC','GD','RAD','GILD','GILD','MMM','TWX','CHTR','CHTR','FB','TRV','COF','FOXA','FOXA','INT','PM','DE','KHC','TECD','AVT','MDLZ','M','ABBV','MCD','DD','NOC','COP','RTN','TSO','ARW','QCOM','PGR','DUK','EPD','AMGN','USFD','USB','AFL','SHLD','DG','AN','CYH','SBUX','LLY','IP','THC','ABT','DLTR','WHR','LUV','EMR','SPLS','PAGP','PAG','UNP','DHR','SO','MAN','BMY','MO','FLR','KSS','LEA','JBL','HIG','TMO','KMB','MOH','PCG','SVU','CMI','CTL','ACM','XRX','MAR','PCAR','GIS','PNC','AEP','IEP','NUE','NEE','PFGC','PBF','HAL','KMX','FCX','WFM','BK','GPS','OMC','GPC','DVA','CL','PPG','GT','SYF','DISH','V','JWN','INTL','WRK','XPO','ARMK','CBS','AES','WCG','FE','CAG','SNX','CDW','TXT','WM','ITW','ODP','MON','CTSH','TXN','LNC','NWL','NWL','MMC','ECL','CHRW','L','CBG','KMI','K','WDC','WDC','ROST','LB','JCP','JCP','RAI','VIAB','BDX','MU','PFG','ARNC','NRG','VFC','DVN','DHI','BBBY','ED','EIX','SHW','NGL','D','AMP','ADP','HLT','FDC','HSIC','HSIC','BBT','RGA','CORE','BIIB','LVS','SWK','PH','SYK','EL','CELG','BLK','XEL','CSX','UNM','JEC','LEN','GPI','LUK','ETR','PYPL','AMAT','VOYA','MA','QVCA','AZO','STT','DTE','LLL','HFC','PX','UHS','DFS','OXY','X','SRE','BAX','GWW','ALV','NSC','BHI','ALLY','SAH','OMI','HUN','LH','MUSA','AAP','FNF','APD','HRL','HTZ','MGM','GLW','RSG','AA','FIS','FIS','STI','LKQ','BWA','BLL','CST','PEG','EMN','EBAY','MHK','OKE','FTR','NFLX','NFLX','NFLX','EXPE','LAD','CAR','RS','GME','TEN','ORLY','ORLY','UNFI','CRM','BSX','NEM','GNW','LYV','VRTV','NWSA','CCK','GLP','PVH','LVLT','NAV','UNVR','CPB','DKS','WY','WY','CHK','APC','IPG','SJM','STLD','FL','FL','SPTN','DF','ZBH','PHM','WRB','PWR','EOG','SCHW','ES','AXE','EME','AIZ','CNP','HRS','HDS','PPL','DGX','WMB','WEC','HSY','AGCO','RL','MAS','WCC','LPNT','NOV','KND','MOS','ADS','ADS','HII','LDOS','LDOS','TSLA','ASNA','DRI','DRI','NVDA','RRD','FITB','Q','JLL','DOV','SPR','R','AMRK','TSCO','SEE','SEE','YUMC','CPN','OI','TRGP','JBLU','JBLU','BEN','ATVI','JBHT','STZ','NCR','ABG','AFG','DISCA','BERY','SANM','CAA','DPS','DDS','HRG','CMS','CMS','BLDR','YUM','CASY','APH','OSK','IHRT','THS','Y','EXPD','AVY','AEE','HBI','MSI','MSI','HOG','RF','ICE','ALK','ORI','LRCX','AKS','ROK','ADBE','AVP','TEX','DAN','RLGY','AMT','PKG','CFG','URI','CLX','GEN','MTB','INGR','UGI','OC','SPGI','WYN','AJG','BURL','FAF','SYMC','PDCO','OLN','NTAP','RJF','TA','FISV','HST','NSIT','MAT','AFSI','CINF','SPG','WU','KEY','DK','BAH','CC','CC','CE','WIN','SEB','ESND','APA','APA','KELYA','LSXMA','RHI']
    symbols = ['MSTR', 'DATA', 'GS', 'JNPR', 'WMT','AAPL','XOM','MCK','UNH','CVS','GM','T','F','ABC','AMZN','GE','VZ','CAH','COST','WBA','KR', 'CVX','FNMA','JPM','ESRX','HD','BA','WFC','BAC','GOOGL','MSFT','ANTM','C','CMCSA','IBM','IBM','PSX','JNJ','PG','VLO','TGT','FMCC','LOW','LOW','MET','AET','PEP','ADM','UPS','INTC','PRU','PRU','UTX','MPC','DIS','HUM','PFE','AIG','LMT','SYY','FDX','HPE','CSCO','HPQ','DOW','HCA','KO','KO','CNC','AAL','AAL','MRK','CI','DAL','BBY','HON','CAT','CAT','MS','MS','GS','ETE','ETE','ORCL','TSN','UAL','ALL','ALL','AXP','TJX','NKE','EXC','GD','RAD','GILD','GILD','MMM','TWX','CHTR','CHTR','FB','TRV','COF','FOXA','FOXA','INT','PM','DE','KHC','TECD','AVT','MDLZ','M','ABBV','MCD','DD','NOC','COP','RTN','TSO','ARW','QCOM','PGR','DUK','EPD','AMGN','USFD','USB','AFL','SHLD','DG','AN','CYH','SBUX','LLY','IP','THC','ABT','DLTR','WHR','LUV','EMR','SPLS','PAGP','PAG','UNP','DHR','SO','MAN','BMY','MO','FLR','KSS','LEA','JBL','HIG','TMO','KMB','MOH','PCG','SVU','CMI','CTL','ACM','XRX','MAR’]
    schedule.every(1).second.do(fetch_price_for_all_symbols, producer, symbols)

    atexit.register(shutdown_hook, producer)

    # setup proper shutdown_hook
    while True:
        schedule.run_pending()
        time.sleep(1)
