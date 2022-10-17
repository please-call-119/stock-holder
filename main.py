# coding=gbk         #说明对应的编码格式，不然容易报错
import json  # python自带的json数据库
import os  # 引用os库
import time  # 调用时间函数
import pandas as pd  # 调用pandas库命名为pd
from random import randint  # python自带的随机数库
from urllib.request import urlopen  # python自带爬虫库,抓取网页数据

num = 0
pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示5000行数
file_location = r'D:/tushare/stock'  # 把数据所在的路径转换成一个字符串并赋值取名

rename_dict = {'SECUCODE': '股票代码', 'REPORT_DATE': '披露时间', 'HOLDER_CODE': '持股机构代码',
               'HOLDER_NAME': '持股机构名称', 'PARENT_ORGCODE_OLD': '持股机构父级代码',
               'PARENT_ORG_NAME': '持股机构父级名称',
               'ORG_TYPE': '持股机构类型', 'TOTAL_SHARES': '持股机构持股总数', 'HOLD_MARKET_CAP': '持股机构持股市值',
               'TOTAL_SHARES_RATIO': '持股机构持占总股本比例',
               'FREE_SHARES_RATIO': '持股机构持占流通本比例'}  # 创建字典，修改名称


def _random(n):
    """
    创建一个n位的随机整数
    :传入参数:n，表示随机数最大值
    :返回随机数
    """
    start = 20  # 最小数
    end = n  # 最大数
    return randint(start, end)  # 调用randint函数在20到n之间进行随机，返回一个整数


def get_content_form_internet(url, max_try_num=10, sleep_time=5):
    # 使用python自带的urlopen函数，从网页上抓取数据
    # url参数代表网址，max_try_num代表最大抓取次数，sleep_time代表抓取失败后停顿的时间
    # return返回抓取到的主页内容
    global free_content
    get_success = False  # 设置一个布尔参数表示抓取成功与否
    for i in range(max_try_num):  # for循环，循环次数为max_try_num传入的值
        try:  # try函数和except函数联用
            free_content = urlopen(url=url, timeout=10).read().decode(
                'utf-8')  # 调取urlopen函数抓取对应网址信息，抓取时间为10秒，超过则报错，用read函数把返回内容写入content
            get_success = True  # 把get_success设置为True
            break  # 跳出for循环向下运行代码
        except Exception as e:  # 如果try函数运行出了错(也就是最大次数超过了依旧没抓到数据)，收集错误数据命名为e
            print('抓取数据报错，次数：', i + 1, '报错内容：', e)  # 显示抓取的次数和错误信息e
            time.sleep(sleep_time)  # 调用停顿函数time.sleep，停顿时间由输入的参数sleep_time决定
    if get_success:  # 如果get_success为true
        return free_content  # 返还content参数（也就是抓取的网页数据）
    else:  # 如果get_success不为true
        raise ValueError('使用urlopen抓取网页数据不断报错，达到尝试上限，停止程序，请尽快检查问题所在')  # 报错，停止程序，等待维护者处理


for root, dirs, files in os.walk(
        file_location):  # 循环调用os.walk函数（遍历目录）来读取股票数据目录，得到三个参数：root: 当前所在的文件夹路径dirs：当前文件夹路径下的文件夹列表files：当前文件夹路径下的文件列表

    for file_name in files:  # 循环读取每个文件名
        if file_name.endswith('.csv'):  # 如果文件名字结尾是.csv
            stock_file = file_name.split(
                '.')  # 把读取的股票文件名称用.分开，转为列表元素，比如第一个文件是000001.SZ.csv，以.为分割符号，转为列表元素’000001‘，‘SZ’，‘csv’
            stock_code = stock_file[0]  # 取列表第一个元素，比如000001，也就是股票代码赋值给stock_code
            stock_files_name = stock_file[0] + '.' + stock_file[1]  # 取列表第一个和第二个元素，中间用.连接，组成文件名（无后缀）
            print(stock_files_name)
            raw_url = 'https://data.eastmoney.com/dataapi/zlsj/detail?SHType=0&SHCode=&SCode={}&ReportDate=2022-06-30&sortField=TOTAL_SHARES&sortDirec=1&pageNum=1&pageSize=30&p=1&pageNo=1&pageNumber=1'.format(
                stock_code)  # 东方财富股票信息抓取网址，用format函数填入对应{}中组成完整抓取网址
            content = get_content_form_internet(raw_url)  # 调用函数抓取信息
            if content == '[]':  # 如果返回的只有[]说明信息为空，该股票已退市，跳出此次循环不执行下面语句，进行下一轮循环
                continue
            content = json.loads(content)['data']  # 返回的是json格式转为字典，提取data对应的value数据赋值给content
            for msg in content:  # 循环提取content中对应的数据
                Holder_df = pd.DataFrame(content)  # 把数据写入表格数据库Holder_df
                Holder_df.rename(columns=rename_dict, inplace=True)  # 调用之前创建的字典，重命名数据库列名，把他转为中文
                Holder_df = Holder_df[
                    ['股票代码', '披露时间', '持股机构代码', '持股机构名称', '持股机构父级代码', '持股机构父级名称',
                     '持股机构类型', '持股机构持股总数', '持股机构持股市值', '持股机构持占总股本比例',
                     '持股机构持占流通本比例']]  # 把需要的列单独提出来组成表格数据库（有很多无效列用不上）
            stock_files = r'D:/tushare/stock_holder/' + stock_files_name + '.csv'  # 存储信息文件的绝对路径和文件名
            Holder_df.to_csv(stock_files, index=False, encoding='gbk')  # 把数据写入文件
            num = num + 1  # 计数器加1
            random_num = _random(60)  # 调用随机函数，传入参数（最大等待秒数为60秒），得到随机停顿秒数random_num
            print('停止{}秒后继续下此开始'.format(random_num))
            time.sleep(random_num)  # 停顿一下，防止抓取频繁而遭反爬
