import baostock as bs
import datetime
from stock_data import get_day_level_data
from stock_data import get_stock_list
from stock_data import get_nearest_trade_day
from stock_data import get_industry
from stock_data import get_profit

from storage import get_mysql_engine
from storage import need_update_table
from storage import read_stock_list
from storage import read_table_exist
from storage import read_last_date_of_stock
from storage import read_stock

from analysis import get_ma_of_kline
from analysis import get_ma_of_volume

from constants import NUM_RECORD, MIN_IPO_DAYS, BLACK_LIST, BLACK_INDUSTRY, BLACK_KEYWORD
from utils import Stock
import multiprocessing
from multiprocessing import Pool


class Pipeline(object):
    def __init__(self):
        self.sql_engine = get_mysql_engine()
        self.stock_list_update_interval = 30
        self.stock_list_table_name = 'basic_stock_list'
        self.min_has_ipo_days = 60

    def init_stock_list(self):
        need_update = need_update_table(
            sql_engine=self.sql_engine,
            update_interval=self.stock_list_update_interval,
            table_name=self.stock_list_table_name)
        if need_update:
            print('=====Start to update stock list')
            stock_list = get_stock_list()
            stock_list.to_sql(
                name=self.stock_list_table_name,
                con=self.sql_engine,
                if_exists='replace',
                index=False
            )

    def get_stock_basic_info(self):
        stock_list = read_stock_list(self.sql_engine, self.stock_list_table_name)
        result = []
        for _, stock_data in stock_list.iterrows():
            stock = Stock(stock_data['code'], stock_data['code_name'])
            stock_type = int(stock_data['stock_type'])  # 1：股票，2：指数,3：其它
            if stock_type == 3:
                print('=====Skipping stock [%s] because type is 3' % stock_data['code_name'])
                continue
            stock.is_stock = (stock_type == 1)

            stock_status = int(stock_data['stock_status'])  # 1：上市，0：退市
            stock.is_out = (stock_status == 0)

            ipoDate = stock_data['ipoDate']
            stock.set_ipo_date(ipoDate)
            result.append(stock)
        return result

    def update_daily_data(self, stocks):
        i = 0
        nearest_trade_day = get_nearest_trade_day()
        nearest_trade_day = datetime.datetime.strptime(nearest_trade_day, '%Y-%m-%d').date()
        for stock in stocks:
            print('=====Loading kline of stock [%s], processed %d' % (stock.name, i))
            table_name = 'stock_' + stock.code[:2] + '_' + stock.code[3:]
            exist = read_table_exist(self.sql_engine, table_name)
            today = datetime.date.today()
            if not exist:
                last_day = today - datetime.timedelta(days=NUM_RECORD)
            else:
                last_day = read_last_date_of_stock(self.sql_engine, table_name)
                if last_day is None:
                    last_day = today - datetime.timedelta(days=NUM_RECORD)

            date_diff = (nearest_trade_day - last_day).days
            if date_diff < 0:
                print('=====Skipping stock [%s] because we have just updated' % stock.name)
                continue

            kline = get_day_level_data(stock.code, str(last_day), str(today))
            if kline is None:
                print('=====Skipping stock [%s] because get_day_level_data returns None' % stock.name)
                continue

            kline.to_sql(
                name=table_name,
                con=self.sql_engine,
                if_exists='append',
                index=False
            )
            i += 1

    @staticmethod
    def login():
        lg = bs.login()
        if int(lg.error_code) != 0:
            print('=====login respond error_code:', lg.error_code)
            print('=====login respond error_msg:', lg.error_msg)
            assert False

    @staticmethod
    def logout():
        bs.logout()

    def do_filter(self, stocks):
        industry_dict = get_industry()
        result = []
        for stock in stocks:
            if stock.has_ipo_days < MIN_IPO_DAYS:
                print('=====Skip [%s], 上市时间[%d]小于%d' % (stock.name, stock.has_ipo_days, MIN_IPO_DAYS))
                continue
            if not stock.is_stock:
                print('=====Skip [%s], 板块指数' % stock.name)
                continue
            if stock.is_out:
                print('=====Skip [%s], 已退市' % stock.name)
                continue
            if stock.code[3:].startswith('688'):
                print('=====Skip [%s], 不关注科创板' % stock.name)
                continue

            if industry_dict is not None and stock.code in industry_dict:
                if industry_dict[stock.code] in BLACK_INDUSTRY:
                    print('=====Skip [%s], 不关注%s板块' % (stock.name, industry_dict[stock.code]))
                    continue
            if stock.name in BLACK_LIST:
                print('=====Skip [%s], 此股位于黑名单' % stock.name)
                continue

            for kw in BLACK_KEYWORD:
                if kw in stock.name:
                    print('=====Skip [%s], 不关注%s板块' % (stock.name, kw))
                    continue

            table_name = 'stock_' + stock.code[:2] + '_' + stock.code[3:]
            df = read_stock(self.sql_engine, table_name)
            if len(df) == 0:
                print('=====Skip [%s], 未读取到数据' % stock.name)
                continue

            isST = int(df['isST'][0])
            if isST == 1:
                print('=====Skip [%s], 已ST' % stock.name)
                continue

            tradestatus = int(df['tradestatus'][0])
            if tradestatus != 1:
                print('=====Skip [%s], 已停牌' % stock.name)
                continue

            last_volumes = []
            last_pctChg = []
            last_close_price = []
            last_amount = []
            day_interval = 3
            for idx, row in df.iterrows():
                if row['volume'] != '':
                    volume = float(row['volume'])
                    last_volumes.append(volume)
                if row['pctChg'] != '':
                    pctChg = float(row['pctChg'])
                    last_pctChg.append(pctChg)
                if row['close'] != '':
                    close_price = float(row['close'])
                    last_close_price.append(close_price)
                if row['amount'] != '':
                    amount = float(row['amount'])
                    last_amount.append(amount)
                if idx == (day_interval - 1):
                    break

            volume_ma50 = get_ma_of_volume(df)
            # ma5 = get_ma_of_kline(df, days=5)
            ma13 = get_ma_of_kline(df, days=13)
            ma21 = get_ma_of_kline(df, days=21)

            if max(last_close_price) >= 400:
                print('=====Skip [%s], 最近股价超过400元' % stock.name)
                continue
            if max(last_close_price) <= 3:
                print('=====Skip [%s], 最近股价低于3元' % stock.name)
                continue

            if last_pctChg[0] < -2:
                print('=====Skip [%s], 今日跌幅超过2个点' % stock.name)
                continue

            if last_pctChg[0] > 8:
                print('=====Skip [%s], 今日涨幅超过8个点' % stock.name)
                continue

            if last_amount[0] < 300000000:
                print('=====Skip [%s], 今日成交量小于3亿' % stock.name)
                continue

            if max(last_volumes) < volume_ma50:
                print('=====Skip [%s], 最近%d天成交量均小于MA50' % (stock.name, day_interval))
                continue
            if max(last_pctChg) <= 0:
                print('=====Skip [%s], 最近%d天连跌' % (stock.name, day_interval))
                continue
            if max(last_close_price) < ma13:
                print('=====Skip [%s], 最近%d天收盘价都低于MA13' % (stock.name, day_interval))
                continue
            if max(last_close_price) < ma21:
                print('=====Skip [%s], 最近%d天收盘价都低于MA21' % (stock.name, day_interval))
                continue

            profit = get_profit(stock.code)
            if profit is not None and len(profit) == 1:
                if profit['liqaShare'][0] != '':
                    liqaShare = float(profit['liqaShare'][0])
                    total_value = (last_close_price[0] * liqaShare) / 100000000.0
                    total_value = round(total_value, 2)
                    if total_value < 100:
                        print('=====Skip [%s], 市值%.2f亿小于100亿' % (stock.name, total_value))
                        continue
                if profit['netProfit'][0] != '':
                    netProfit = float(profit['netProfit'][0])
                    netProfit = netProfit / 100000000.0
                    netProfit = round(netProfit, 2)
                    if netProfit < 0.5:
                        print('=====Skip [%s], 季报净利润%.2f亿低于5千万' % (stock.name, netProfit))
                        continue

            if len(last_amount) >= 2:
                if last_amount[0] / last_amount[1] >= 2:
                    stock.reason += ' 倍量'
            result.append(stock)

        return result

    def run(self, update_daily, num_thread=8):
        self.login()
        self.init_stock_list()
        stocks = self.get_stock_basic_info()
        if update_daily:
            pool = Pool(num_thread)
            num_per_process = len(stocks) // num_thread
            stocks_process = [stocks[i:i + num_per_process] for i in range(0, len(stocks), num_per_process)]
            pool.map(self.update_daily_data, stocks_process)
        result = self.do_filter(stocks)
        self.logout()
        return result
