import pandas as pd
from pandas.io.data import Options
import requests
from bs4 import BeautifulSoup
import itertools
import urllib3
import datetime

###Get SP500 Tickers
def scrape_list(site):
    hdr = {'User-Agent': 'Mozilla/5.0'}
    http = urllib3.PoolManager()
    req = http.request('GET', site, headers=hdr)
    page = req.data
    # print(page)
    soup = BeautifulSoup(page, "html.parser")
    table = soup.find('table', {'class': 'wikitable sortable'})
    sector_tickers = dict()
    for row in table.findAll('tr'):
        col = row.findAll('td')
        if len(col) > 0:
            sector = str(col[3].string.strip()).lower().replace(' ', '_')
            ticker = str(col[0].string.strip())
            if sector not in sector_tickers:
                sector_tickers[sector] = list()
            sector_tickers[sector].append(ticker)
    return sector_tickers

site = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
list_of_SP500 = scrape_list(site)
sectors = list_of_SP500.keys()
ticker_names = list(itertools.chain(*list_of_SP500.values()))
time = datetime.datetime.now().strftime("%Y%m%d")
####SPX and VIX dataframes


VIX = pd.DataFrame(Options('^vix', 'yahoo').get_all_data())
SPX = pd.DataFrame(Options('^spxpm', 'yahoo').get_all_data())

VIX.to_csv("VIX_Options_" + time + '.csv')
SPX.to_csv("SPX_Options_" + time + '.csv')
####SP500 companies options data
####combined in 1 dataframe


# SP500_dataframes = []
#
# for i in ticker_names:
#     try:
#         SP500_dataframes.append(pd.DataFrame(Options(i, 'yahoo').get_all_data()))
#     except:
#         pass
#
# SP500_dataframe_final = pd.concat(SP500_dataframes)
# SP500_dataframe_final.to_csv("SP500_Options_" + time + '.csv')

