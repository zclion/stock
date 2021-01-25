def format_volume(value):
    volume = float(value)
    volume = round(volume / 100.0)
    volume_len = len(str(volume))
    if volume_len <= 4:
        volume = str(volume) + '手'
    elif volume_len <= 8:
        volume = volume / 10000.0
        if volume_len in (5, 6):
            keep_float = 2
        else:
            keep_float = 1
        volume = round(volume, keep_float)
        volume = str(volume) + '万手'
    else:
        volume = volume / 100000000.0
        volume = round(volume, 2)
        volume = str(volume) + '亿手'
    return volume


def format_amount(value):
    amount = float(value)
    amount = int(round(amount))
    amount_len = len(str(amount))
    if amount_len <= 4:
        amount = str(amount)
    elif amount_len <= 8:
        amount = amount / 10000.0
        if amount_len in (5, 6):
            keep_float = 2
        else:
            keep_float = 1
        amount = round(amount, keep_float)
        amount = str(amount) + '万'
    else:
        amount = amount / 100000000.0
        amount = round(amount, 2)
        amount = str(amount) + '亿'
    return amount


def format_percent(value):
    result = float(value)
    result = round(result, 2)
    result = str(result) + '%'
    return result


def get_ma_of_kline(df, days):
    assert days in (5, 13, 21, 34)
    ma = 0.0
    for idx, row in df.iterrows():
        if row['close'] == '':
            continue
        close_price = float(row['close'])
        ma += close_price
        if idx == (days - 1):
            break
    result = ma / float(days)
    result = round(result, 2)
    return result


def get_ma_of_volume(df, days=50):
    ma = 0.0
    for idx, row in df.iterrows():
        if row['volume'] == '':
            continue
        volume = float(row['volume'])
        ma += volume
        if idx == (days - 1):
            break
    result = ma / float(days)
    result = round(result)
    return result


def parse(row_data):
    '''
    is_st = row_data[query_items_dict['isST']]
    trade_status = row_data[query_items_dict['tradestatus']]
    # format volume
    volume = row_data[query_items_dict['volume']]
    row_data[query_items_dict['volume']] = format_volume(volume)
    # format amount
    amount = row_data[query_items_dict['amount']]
    row_data[query_items_dict['amount']] = format_amount(amount)
    # format turn
    turn = row_data[query_items_dict['turn']]
    row_data[query_items_dict['turn']] = format_percent(turn)
    # format pctChg
    pctChg = row_data[query_items_dict['pctChg']]
    row_data[query_items_dict['pctChg']] = format_percent(pctChg)
    '''
    pass
