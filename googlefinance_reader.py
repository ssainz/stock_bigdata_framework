import json
import requests
from time import gmtime, strftime


def getQuotes(symbol, str_time=None):

    if str_time is None:
        str_time = strftime("%Y-%m-%dT%H:%M:%SZ", gmtime())

    rsp = requests.get('https://finance.google.com/finance?q='+symbol+'&output=json')
    if rsp.status_code in (200,):

        # This magic here is to cut out various leading characters from the JSON
        # response, as well as trailing stuff (a terminating ']\n' sequence), and then
        # we decode the escape sequences in the response
        # This then allows you to load the resulting string
        # with the JSON module.
        fin_data = json.loads(rsp.content[6:-2].decode('unicode_escape'))


        # print out some quote data
        to_kafka = {
            'LastTradeWithCurrency': fin_data['l'].encode('ascii', 'ignore'),
            'LastTradeDateTime': str_time,
            'StockSymbol': fin_data['symbol'].encode('ascii', 'ignore')
        }
        return to_kafka


if __name__ == '__main__':

    print(getQuotes('MSTR'))
