from selector import Pipeline
from utils import send_to_dingtalk

if __name__ == '__main__':
    pipeline = Pipeline()
    result = pipeline.run(update_daily=True)
    print('===========================================')
    msg = ''
    i = 0
    for stock in result:
        print('result: %s' % stock.name)
        per_msg = '%d %s %s\n' % (i, stock.name, stock.reason)
        msg += per_msg
        i += 1
    print('selected %d stocks' % len(result))
    msg += 'total num: %d' % len(result)
    send_to_dingtalk(msg)
    print('===========================================')
