from selector import Pipeline
from utils import send_to_dingtalk

if __name__ == '__main__':
    pipeline = Pipeline()
    result = pipeline.run()

    print('===========================================')
    msg = ''
    for stock in result:
        print('result: %s' % stock.name)
        msg += (stock.name + '\n')
    print('selected %d stocks' % len(result))
    send_to_dingtalk(msg)
    print('===========================================')
