from sqlalchemy import create_engine
import pandas as pd
import datetime


# create db: CREATE DATABASE IF NOT EXISTS stock DEFAULT CHARACTER SET utf8;
def get_mysql_engine(db_name='stock', user='root', psw='root', host='localhost'):
    engine = create_engine(
        'mysql+pymysql://{0}:{1}@{2}/{3}?charset=utf8'.format(user, psw, host, db_name),
        encoding='utf-8'
    )
    return engine


def read_table_exist(sql_engine, table_name):
    sql = "select * from information_schema.tables where table_name='%s'" % table_name
    with sql_engine.engine.connect() as conn, conn.begin():
        df = pd.read_sql_query(sql, sql_engine)

    if len(df) == 0:
        return False
    else:
        return True


def need_update_table(sql_engine, update_interval, table_name):
    exist = read_table_exist(sql_engine, table_name)
    if not exist:
        print('=====Need to update tabel [%s] because table is empty' % table_name)
        return True

    sql = 'select update_date from %s limit 1' % table_name
    with sql_engine.engine.connect() as conn, conn.begin():
        df = pd.read_sql_query(sql, sql_engine)

    assert len(df) == 1
    last_update_date = df['update_date'][0]
    last_update_date = datetime.datetime.strptime(last_update_date, '%Y-%m-%d').date()
    today = datetime.date.today()
    date_diff = (today - last_update_date).days
    print('=====Last update date of table [%s] is [%s]' % (table_name, last_update_date))
    if date_diff < update_interval:
        print('=====No need to update table [%s]' % table_name)
        return False
    else:
        print('=====Need to update table [%s]' % table_name)
        return True


def read_stock_list(sql_engine, table_name):
    sql = 'select * from %s' % table_name
    with sql_engine.engine.connect() as conn, conn.begin():
        df = pd.read_sql_query(sql, sql_engine)
    return df


def read_last_date_of_stock(sql_engine, table_name):
    sql = 'select * from %s order by date desc limit 1' % table_name
    with sql_engine.engine.connect() as conn, conn.begin():
        df = pd.read_sql_query(sql, sql_engine)

    if len(df) == 0:
        return None
    assert len(df) == 1

    last_update_date = df['date'][0]
    last_update_date = datetime.datetime.strptime(last_update_date, '%Y-%m-%d').date()
    last_update_date = last_update_date + datetime.timedelta(days=1)
    return last_update_date


def read_stock(sql_engine, table_name):
    sql = 'select * from %s order by date desc' % table_name
    with sql_engine.engine.connect() as conn, conn.begin():
        df = pd.read_sql_query(sql, sql_engine)
    return df


def set_index():
    engine = get_mysql_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()

    cursor.execute('show tables;')
    tables = cursor.fetchall()
    result = []
    for table in tables:
        table = str(table)
        table = table[2:]
        table = table[:-3]
        if table == 'basic_stock_list':
            print('skip basic_stock_list')
            continue
        result.append(table)

    i = 0
    for table_name in result:
        print('=====' + table_name, i)
        s1 = 'ALTER TABLE %s MODIFY COLUMN date varchar(255);' % table_name
        cursor.execute(s1)
        result = cursor.fetchall()

        s2 = 'ALTER TABLE `%s` ADD UNIQUE (`date`);' % table_name
        cursor.execute(s2)
        result = cursor.fetchall()
        i += 1

    cursor.close()
    conn.close()
