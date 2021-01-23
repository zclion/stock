import baostock as bs
import pandas as pd


def get_day_level_data(stock_code, start_date, end_date):
    adjustflag = str(2)  # 1.后复权 2.前复权 3.不复权
    query_items = 'date,open,high,low,close,volume,amount,turn,pctChg,isST,tradestatus'
    rs = bs.query_history_k_data_plus(stock_code,
                                      query_items,
                                      start_date=start_date,
                                      end_date=end_date,
                                      frequency='d',
                                      adjustflag=adjustflag)
    if int(rs.error_code) != 0:
        print('query_history_k_data_plus respond error_msg:' + rs.error_msg)
        return None

    data_list = []
    while (rs.error_code == '0') & rs.next():
        row_data = rs.get_row_data()
        data_list.append(row_data)
    result = pd.DataFrame(data_list, columns=rs.fields)
    return result
