__author__ = 'achau'
import datetime
import math

import mysql.connector


def test_strategy_simple(cnx, startDate, endDate):
    cursor_a = cnx.cursor(buffered=True)
    away_from_std_atm_iv = 1
    trading_days = 14
    time_denominator = math.sqrt(252 / trading_days)

    query = ("select spx.close spxClose, vix.close vixClose from stock_data spx, stock_data vix "
             "where spx.date between %s AND %s "
             "AND spx.symbol_id = 1 "
             "AND vix.symbol_id = 2 "
             "AND spx.date = vix.date "
             "AND spx.weekly_close = 1 "
             "ORDER BY spx.date asc")

    cursor_a.execute(query, (startDate, endDate))
    order_list = []
    result_list = []
    win = 0
    lose = 0
    for (spx_close, vix_close) in cursor_a:
        flt_spx_close, flt_vix_close = float(spx_close), float(vix_close)

        iv_percent = away_from_std_atm_iv * flt_vix_close / time_denominator
        iv_normalized = iv_percent / 100 * flt_spx_close
        order_list.append([flt_spx_close + iv_normalized, flt_spx_close - iv_normalized])
        if len(order_list) > 3:
            high, low = order_list[-3]
            if low <= flt_spx_close <= high:
                win += 1
                result_list.append([high, low, flt_spx_close, 1])
            else:
                lose += 1
                result_list.append([high, low, flt_spx_close, 0])

    return win / (win + lose)


connection = mysql.connector.connect(user='root', password='albertscottstefan',
                                     host='tradingdb.cuvj8nageqtn.us-west-2.rds.amazonaws.com',
                                     database='options')

start = datetime.date(2012, 1, 1)
end = datetime.date(2015, 6, 30)
win_percent = test_strategy_simple(connection, start, end)
print(win_percent)
connection.close()
