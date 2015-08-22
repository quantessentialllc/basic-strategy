__author__ = 'achau'
import datetime
import math

import MySQLdb

ic_headers = ['options_data_id', 'underyling_symbol', 'quote_date', 'root', 'expiration', 'strike', 'option_type',
              'open', 'high', 'low', 'close', 'trade_volume', 'bid_size', 'bid', 'ask_size', 'ask', 'underlying_bid',
              'underlying_ask', 'implied_underlying_price', 'active_underlying_price', 'implied_volatility', 'delta',
              'gamma', 'theta', 'vega', 'rho', 'bid_size_eod', 'bid_eod', 'ask_size_eod', 'ask_eod',
              'underlying_bid_eod', 'underlying_ask_eod', 'vwap', 'open_interest', 'delivery_code']

wing_strike_span = 5
strike_interval = 6

def test_strategy_simple(cnx, start_date, end_date):
    cursor_a = cnx.cursor()
    cursor_b = cnx.cursor()
    away_from_std_atm_iv = .8
    trading_days = 10
    wing_span = strike_interval * wing_strike_span
    time_denominator = math.sqrt(252 / trading_days)

    query = ('select spx.close, vix.close, spx.monthly_close, spx.date '
             'from stock_data spx, stock_data vix '
             'where spx.date between %s AND %s '
             'AND spx.symbol_id = 1 '
             'AND vix.symbol_id = 2 '
             'AND spx.date = vix.date '
             'AND spx.weekly_close = 1 '
             'ORDER BY spx.date asc')

    cursor_a.execute(query, (start_date, end_date))
    order_list = []
    result_list = []
    win = 0
    lose = 0
    outcome = 0
    for (spx_close, vix_close, monthly, date) in cursor_a.fetchall():
        flt_spx_close, flt_vix_close = float(spx_close), float(vix_close)
        iv_percent = away_from_std_atm_iv * flt_vix_close / time_denominator
        iv_normalized = iv_percent / 100 * flt_spx_close
        put_short = roundx(flt_spx_close - iv_normalized)
        put_long = put_short - wing_span
        call_short = roundx(flt_spx_close + iv_normalized, 'up')
        call_long = call_short + wing_span
        order_list.append([call_long, call_short, put_short, put_long, date.strftime("%Y-%m-%d"), flt_vix_close])
        if len(order_list) > 3:
            l, w, o = compute_outcome(cursor_b, date, flt_spx_close, order_list[-3], result_list, monthly)
            win += w
            lose += l
            outcome += o

    print("Outcome: ", str(outcome))
    return win / (win + lose)


def compute_outcome(cursor, end_date, spx_close, order_info, result_list, monthly):
    ic_query = ('select * from options_data_eod od '
                'where od.strike in (%s, %s, %s, %s) '
                'and od.quote_date in (%s, %s) '
                'and od.expiration=%s '
                'ORDER BY od.option_type, od.strike, od.quote_date')

    expiration_date = end_date
    if is_third_friday(end_date):
        expiration_date = end_date + datetime.timedelta(days=1)
    str_exp_date = expiration_date.strftime("%Y-%m-%d")
    str_end_date = end_date.strftime("%Y-%m-%d")
    call_long, call_short, put_short, put_long, open_date, vol_vix = order_info
    cursor.execute(ic_query,
                   (call_long, call_short, put_short, put_long, open_date, str_end_date, str_exp_date))

    ic_map = {}
    for ic_data in cursor.fetchall():
        d_map = dict(zip(ic_headers, ic_data))
        pk = d_map['quote_date'] + str(int(d_map['strike'])) + d_map['option_type']
        ic_map[pk] = d_map

    if len(ic_map) is not 16:
        # print(call_long, call_short, put_short, put_long, open_date, str_exp_date, str_end_date)
        # print(ic_map)

        ic_query = ("select * from options_data_eod od "
                    "where od.strike in (%s, %s, %s, %s) "
                    "and od.quote_date in ('%s', '%s') "
                    "and od.expiration='%s' "
                    "ORDER BY od.option_type, od.strike, od.quote_date")
        # print(len(ic_map))
        # print(ic_query % (call_long, call_short, put_short, put_long, open_date, str_end_date, str_exp_date))
        return 0, 0, 0

    win, lose = 0, 0
    message = "Date: " + str_end_date + "\t\tPut Short: " + "%.0f" % put_short + "\t\tActual: " + \
              "%.0f" % spx_close + " - " + "%.2f" % vol_vix + "\t\tCall Short: " + "%.0f" % call_short

    pk_call_long = open_date + str(call_long) + 'c'
    pk_call_short = open_date + str(call_short) + 'c'
    pk_put_short = open_date + str(put_short) + 'p'
    pk_put_long = open_date + str(put_long) + 'p'
    open_call_long = ic_map[pk_call_long]['open']
    open_call_short = ic_map[pk_call_short]['open']
    open_put_short = ic_map[pk_put_short]['open']
    open_put_long = ic_map[pk_put_long]['open']
    initial_premium = float(open_call_short - open_call_long + open_put_short - open_put_long) * 100
    max_loss = 100*strike_interval*wing_strike_span - initial_premium

    outcome = initial_premium

    if put_short <= spx_close <= call_short:
        win += 1
        result_list.append([call_short, put_short, spx_close, 0])
        print("[ +++ ", "%.2f" % initial_premium, "\t] ", message)
    else:
        if put_short > spx_close:
            lose += 1
            outcome = initial_premium - ((float(put_short) - spx_close) * 100)
            if put_long >= spx_close:
                outcome = max_loss
                outcome_symbol = '---'
            elif outcome > 0:
                outcome_symbol = ' + '
            elif outcome == 0:
                outcome_symbol = '==='
            else:
                outcome_symbol = ' - '

            print("[", outcome_symbol, "%.2f" % outcome, "\t] ", message, "\t[LOWER]")
            result_list.append([call_short, put_short, spx_close, -1])
        else:
            lose += 1
            outcome = initial_premium - ((float(spx_close) - call_short) * 100)
            if call_long <= spx_close:
                outcome = max_loss
                outcome_symbol = '---'
            elif outcome > 0:
                outcome_symbol = ' + '
            elif outcome == 0:
                outcome_symbol = '==='
            else:
                outcome_symbol = ' - '

            print("[", outcome_symbol, "%.2f" % outcome, "\t] ", message, "\t[UPPER]")
            result_list.append([call_short, put_short, spx_close, 1])

    return lose, win, outcome


def roundx(x, direction='down', base=5):
    if direction == 'down':
        return int(base * math.floor(float(x) / base))
    return int(base * math.ceil(float(x) / base))


def cleanse_data(cnx):
    cursor = cnx.cursor()

    update_weekly = ('UPDATE stock_data sd '
                     'SET sd.weekly_close = 1 '
                     'WHERE DATE(sd.date)=%s')

    update_monthly = ('UPDATE stock_data sd '
                      'SET sd.weekly_close = 1, sd.monthly_close = 1 '
                      'WHERE DATE(sd.date)=%s')

    start = datetime.date(2005, 5, 30)
    end = datetime.date(2015, 7, 16)
    rows_updated = 0

    while start < end:
        if start.isoweekday() == 5:
            if is_third_friday(start):
                monthly = start - datetime.timedelta(days=1)
                date_string = monthly.strftime('%Y-%m-%d')
                cursor.execute(update_monthly, [date_string])
                rows_updated += 1
            else:
                date_string = start.strftime('%Y-%m-%d')
                cursor.execute(update_weekly, [date_string])
                rows_updated += 1
        start += datetime.timedelta(days=1)

    print(rows_updated)
    cnx.commit()


def is_third_friday(d):
    return d.isoweekday() == 5 and 15 <= d.day <= 21


connection = MySQLdb.connect(user='root', passwd='albertscottstefan',
                             host='tradingdb.cuvj8nageqtn.us-west-2.rds.amazonaws.com',
                             db='options')

start = datetime.date(2012, 6, 30)
end = datetime.date(2015, 6, 30)
win_percent = test_strategy_simple(connection, start, end)
print(win_percent)

# cleanse_data(connection)
connection.close()
