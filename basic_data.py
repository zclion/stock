import datetime
import baostock as bs
import pandas as pd


def get_trade_status(day):
    rs = bs.query_trade_dates(start_date=day, end_date=day)
    if int(rs.error_code) != 0:
        print('[query_trade_dates] respond error_code:' + rs.error_code)
        print('[query_trade_dates] respond error_msg:' + rs.error_msg)
        assert False

    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    assert len(result) == 1
    return result['is_trading_day'][0] == '1'


def get_nearest_trade_day():
    date = datetime.date.today()
    while True:
        trade_status = get_trade_status(str(date))
        if trade_status:
            print('=====[%s] is trading day' % str(date))
            break
        date = date - datetime.timedelta(days=1)
    return str(date)


def get_stock_basic_info(stock_code):
    rs = bs.query_stock_basic(code=stock_code)
    if int(rs.error_code) != 0:
        return None

    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())

    result = pd.DataFrame(data_list, columns=rs.fields)
    if len(data_list) == 1:
        return result
    else:
        return None


def get_stock_list():
    date = get_nearest_trade_day()
    rs = bs.query_all_stock(day=date)
    if int(rs.error_code) != 0:
        print('query_all_stock respond error_code:' + rs.error_code)
        print('query_all_stock respond error_msg:' + rs.error_msg)
        assert False

    data_list = []
    i = 0
    while rs.next():
        row_data = rs.get_row_data()
        row_data.append(date)

        stock_code = row_data[0]
        stock_name = row_data[2]
        assert stock_code.startswith('sz.') or stock_code.startswith('sh.')
        print('=====Loading basic info of [%s], processed %d' % (stock_name, i))
        stock_basic_info = get_stock_basic_info(stock_code)
        if stock_basic_info is None:
            print('=====Skipping stock [%s] because get_stock_basic_info error' % stock_name)
            continue
        stock_type = stock_basic_info['type'][0]  # 1：股票，2：指数,3：其它
        stock_status = stock_basic_info['status'][0]  # 1：上市，0：退市
        ipoDate = stock_basic_info['ipoDate'][0]
        outDate = stock_basic_info['outDate'][0]
        row_data += [stock_type, stock_status, ipoDate, outDate]
        data_list.append(row_data)
        i += 1

    basic_info_fields = ['stock_type', 'stock_status', 'ipoDate', 'outDate']
    result = pd.DataFrame(data_list, columns=rs.fields + ['update_date'] + basic_info_fields)
    return result
