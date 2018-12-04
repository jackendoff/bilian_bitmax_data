import requests
import json
import numpy as np
import time
import pandas as pd
from pymysql import connect
import csv
from custom_send_email import sendEmail


# 获取订单簿数据
class GetData(object):
    '''
    获取对应api  bitmax数据，计算固定比例的数据，监控变化，发送邮件
    '''

    def __init__(self,url):
        self.url = url
        # self.data_all = {}
        # 返回的是创建对象的时间
        self.now_time = time.time()
        pass

    def get_data(self):
        # 从api接口获取数据，返回数据列表(嵌套字典)
        r = requests.get(self.url)
        data = json.loads(r.text)
        return data

    def slope(self, data_list, coin_name):
        # 将数据分成sell和buy，分别求斜率(价格步长为0.5时，斜率为本价格仓位数量size).
        data_all = {}
        data_all['sell_XBT'] = []
        data_all['buy_XBT'] = []
        for data_dict in data_list:
            if data_dict['side'] == 'Sell':
                data_all['sell_XBT'].append([data_dict['size'],data_dict['price']])
            elif data_dict['side'] == 'Buy':
                data_all['buy_XBT'].append([data_dict['size'],data_dict['price']])

        # 获取斜率列表（size列表）
        slope_sell = [size[0] for size in data_all['sell_XBT']]
        slope_buy = [size[0] for size in data_all['buy_XBT']]
        # print('****************',slope_sell,slope_buy)

        # 获取斜率列表中处于99%的数据（size列表）
        slope_sell_data = np.percentile(slope_sell, 99)
        slope_buy_data = np.percentile(slope_buy, 99)

        # 将斜率列表中大于99%的数据返回
        data_sell_list = []
        data_buy_list = []
        for data_sell in data_all['sell_XBT']:
            if data_sell[0] > slope_sell_data:
                data_sell = [str(data_sell[0]),str(data_sell[1])]
                data_sell_list.append(data_sell)
                # print('data_sell',data_sell)
        for data_buy in data_all['buy_XBT']:
            if data_buy[0] > slope_buy_data:
                data_buy = [str(data_buy[0]),str(data_buy[1])]
                data_buy_list.append(data_buy)
        # data_list 中的参数为 size price time
        return data_sell_list, data_buy_list

        pass

    def data_to_mysql(self, data_list, database_name, table_name, coin_name):
        # 将获取到的数据列表放到数据库中
        #创建数据库连接
        conn = connect(
            host='localhost',
            port=3306,
            user='root',
            password='1783126868@qq.com',
            database=database_name,
            charset='utf8'
        )

        # 获取游标
        cursor = conn.cursor()
        if coin_name == 'XBT':
            sql = 'delete from '+table_name+' where id>=1;'
            cursor.execute(sql)
            conn.commit()
            sql = 'alter table '+table_name+' AUTO_INCREMENT 1;'
            cursor.execute(sql)
            conn.commit()

        # 将数据变为 MySQL 格式
        for data_dict in data_list:
            # print(data_dict)
            values_data = (data_dict['symbol'],data_dict['side'], data_dict['size'], data_dict['price'])
            # print(type(data_dict['price']))
            # print(values_data)
            sql = '''insert into '''+table_name+''' (symbol,side,size,price) values'''+str(values_data)+''';'''
            # print(sql)
            # 执行sql语句

            try:
                row_count = cursor.execute(sql)
            except:
                print('sql执行失败')

        # 提交到数据库
        conn.commit()
        # 关闭游标
        cursor.close()
        # 关闭连接
        conn.close()
        pass

    def data_to_csv(self, data_sell_list, data_buy_list,  coin_name):
        #将获取到的>99%的斜率列表数据保存到csv文件里
        headers = ['Symbol','Price','Date','Time','Change','Volume']
        # 将buy——list数据放入csv
        rows_buy = data_buy_list
        with open(coin_name+'_buy.csv', 'w') as f:
            f_csv = csv.writer(f)
            # f_csv.writerrow(headers)
            f_csv.writerows(rows_buy)
        # 将 sell_list数据放入csv
        rows_sell = data_sell_list
        with open(coin_name + '_sell.csv', 'w') as f:
            f_csv = csv.writer(f)
            # f_csv.writerrow(headers)
            f_csv.writerows(rows_sell)

    def csv_to_data(self, coin_name):
        # 读取csv文件文件数据，返回coin_name buy 和 sell 数据

        # 打开coin_name+'_buy.csv'文件
        coin_buy_list = []
        with open(coin_name+'_buy.csv') as f:
            f_csv = csv.reader(f)
            for row in f_csv:
                if len(row) == 0:
                    continue
                # if '.' in row[1]:
                #     row[1] = float(row[1])
                # else:
                #     row[1] = int(row[1])
                row = [row[0],row[1]]
                coin_buy_list.append(row)

        # 打开coin_name+'_sell.csv'文件
        coin_sell_list = []
        with open(coin_name+'_sell.csv') as f:
            f_csv = csv.reader(f)
            for row in f_csv:
                if len(row) == 0:
                    continue
                # if '.' in row[1]:
                #     row[1] = float(row[1])
                # else:
                #     row[1] = int(row[1])
                row = [row[0], row[1]]
                coin_sell_list.append(row)
        # print('coin_sell_list:',coin_sell_list,'coin_buy_list:',coin_buy_list)
        return coin_sell_list, coin_buy_list


    def data_change(self,slope_sell,data_sell):
        '''
        参数（二维list）顺序：新数据，老数据。
        返回值：改变的完整数据列表，新增的完整数据列表，减少的完整数据列表
        '''
        change_data = []
        # newly_data = []
        # lessen_data = []
        # 变化的数据
        for slope_x, slope_y in slope_sell:
            for data_x, data_y in data_sell:
                #  价位相等， 仓位数量不同
                if data_y == slope_y and slope_x != data_x:
                    # print('在{0}价位，仓位发生变化{1}----->{2}'.format(data_y,data_x,slope_x))
                    change_data.append([data_y, data_x, slope_x])

        # 新增或减少的数据
        # 价格遍布不同

        # 价位列表
        slope_price_list = [temp[1] for temp in slope_sell]
        data_price_list = [temp[1] for temp in data_sell]

        # 新增的价位列表
        newly_price_data = list(set(slope_price_list) - set(data_price_list))
        # 新增的完整数据
        newly_data = [temp for temp in slope_sell if temp[1] in newly_price_data]

        # 减少的价位列表
        lessen_price_data = list(set(data_price_list) - set(slope_price_list))
        # 减少的完整数据
        lessen_data = [temp for temp in data_sell if temp[1] in lessen_price_data]

        # change_data = []
        # newly_data = []
        # lessen_data = []

        return change_data,newly_data,lessen_data

    def compare(self, slope_sell, data_sell, slope_buy, data_buy):
        # 比较两个列表数据前后的变化，返回变化的数据列表
        change_sell_data,newly_sell_data,lessen_sell_data  = self.data_change(slope_sell, data_sell)
        content_sell_list = [change_sell_data,newly_sell_data,lessen_sell_data]


        # 比较buy数据 slope_buy data_buy
        change_buy_data,newly_buy_data,lessen_buy_data  = self.data_change(slope_buy, data_buy)
        content_buy_list = [change_buy_data,newly_buy_data,lessen_buy_data]

        return content_sell_list, content_buy_list

    def unpack(self,special_list):
        # 将列表解包，并合成字符串
        # 解包修改的数据
        content = ''
        for data in special_list[0]:
            change_str = '{0}价位的仓位数据发生变化：{1}--->{2}\n'.format(data[0], data[1], data[2])
            content += change_str
        # 解包新增的数据
        for data in special_list[1]:
            newly_str = '{0}价位的仓位数据发生变化：{1}--->{2}\n'.format(data[1], '0', data[0])
            content += newly_str
        # 解包消失的数据
        for data in special_list[2]:
            lessen_str = '{0}价位的仓位数据发生变化：{1}--->{2}\n'.format(data[1], data[0], '0', data[0])
            content += lessen_str
        return content

    def send_email(self,coin_name, sell_list, buy_list):
        # 将数据解包并发送邮件
        title = coin_name+' order_book_sell 数据发生变化'
        content = self.unpack(sell_list)
        sendEmail(content,title)

        title = coin_name+' order_book_buy 数据发生变化'
        content = self.unpack(buy_list)
        sendEmail(content,title)


if __name__ == '__main__':

    # 从bitmex交易所获取不同币种的数据。
    coin_list = ['XBT', 'ADA', 'BCT', 'EOS', 'ETH', 'LTC', 'TRX', 'XRP']
    # coin_list = ['XBT']
    all_num = 0
    for coin_name in coin_list:

        url = 'https://www.bitmex.com/api/v1/orderBook/L2?symbol='+coin_name+'&depth=0'
        # 创建对象
        ob = GetData(url)
        # 获取url数据
        data = ob.get_data()
        all_num+=len(data)
        print('*'*50,all_num, coin_name)
        if len(data) == 0:
            print(coin_name, '未获取到数据')
            continue

        # 将数据存入MySQL数据库
        # try:
        #     ob.data_to_mysql(data,database_name='bitmex', table_name='order_book',coin_name=coin_name)
        # except:
        #     print('存入数据失败')

        # 获取斜率大于指定值的完整数据列表
        slope_sell, slope_buy = ob.slope(data, coin_name)

        # 从csv文件里读取数据（曾经的数据，大于固定斜率的数据）
        data_sell, data_buy = ob.csv_to_data(coin_name)
        # 将刚获取的斜率列表和csv数据进行对比
        content_sell_list, content_buy_list = ob.compare(slope_sell,data_sell,slope_buy,data_buy)
        ob.send_email(coin_name,content_sell_list,content_buy_list)
        # 将斜率大于指定值的列表放到csv文件里
        ob.data_to_csv(slope_sell, slope_buy, coin_name)
        # print(slope_sell,slope_buy)



