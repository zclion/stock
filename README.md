策略TODO：
1. 剔除市值小于80亿
2. 剔除亏损和业绩差

mysql:
1. 设置主键：
1.1 ALTER TABLE basic_stock_list MODIFY COLUMN code varchar(255);
1.2 ALTER TABLE `basic_stock_list` ADD UNIQUE (`code`);
1.3 run set_index in storage.py

