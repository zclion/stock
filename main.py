import baostock as bs
import datetime
from stock_data import get_day_level_data
from basic_data import get_stock_list, get_nearest_trade_day
from storage import get_mysql_engine
from storage import need_update_table, read_stock_list, read_table_exist, read_last_date_of_stock
from constants import NUM_RECORD


class Stock(object):
    def __init__(self, code, name):
        self.code = code
        self.name = name
        self.ipo_date = None

    def set_ipo_date(self, date):
        self.ipo_date_str = date
        self.ipo_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
        today = datetime.date.today()
        self.has_ipo_days = (today - self.ipo_date).days


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
            table_name = 'stock_' + stock.code[:2]+'_'+stock.code[3:]
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

    def run(self):
        self.login()
        self.init_stock_list()
        stocks = self.get_stock_basic_info()
        self.update_daily_data(stocks)
        self.logout()


if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run()
