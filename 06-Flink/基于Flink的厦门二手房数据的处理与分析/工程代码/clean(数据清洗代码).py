import pandas as pd
import cn2an

housedata = pd.read_csv("D:/新建文件夹 (6)/基于Flink的厦门二手房数据的处理与分析/基于Flink的厦门二手房数据的处理与分析/数据集/xmhouse_data.csv")

# 将total_price转化为数字
housedata['total_price'] = housedata['total_price'].str.replace('[', '').str.replace(']', '').str.replace('\'','')
housedata['total_price'] = pd.to_numeric(housedata['total_price'],errors='ignore')

# 将listing_day和last_trade转化为日期
housedata['listing_day'] = housedata['listing_day'].apply(lambda x: pd.to_datetime(x.replace('[', '').replace(']', '').replace('\'','')) if x != '[\'暂无数据\']' else pd.NaT)
housedata['last_trade'] = housedata['last_trade'].apply(lambda x: pd.to_datetime(x.replace('[', '').replace(']', '').replace('\'','')) if x != '[\'暂无数据\']' else pd.NaT)

# 去除unit_price字段中的‘元/平’
housedata['unit_price'] = housedata['unit_price'].str.replace('[', '').str.replace(']', '').str.replace('\'','').str.replace('元/平', '').str.split(',').str.join('')
housedata['unit_price'] = pd.to_numeric(housedata['unit_price'],errors='ignore')

# 去除building_area字段中的‘平米’
housedata['building_area'] = housedata['building_area'].str.replace('[', '').str.replace(']', '').str.replace('\'','').str.replace('平米', '')
housedata['building_area'] = pd.to_numeric(housedata['building_area'],errors='ignore')

# 去除build_year字段中的‘年建’
housedata['build_year'] = housedata['build_year'].str.replace('[', '').str.replace(']', '').str.replace('\'','').str.replace('年建', '')
housedata['build_year'] = housedata['build_year'].str.replace('未知', '-1')
housedata['build_year'] = pd.to_numeric(housedata['build_year'],errors='ignore')


# 选取chaoxiang字段中的主朝向
housedata['chaoxiang'] = housedata['chaoxiang'].str.replace('[', '').str.replace(']', '').str.replace('\'','').str.split(' ')
housedata['chaoxiang'] = housedata['chaoxiang'].str[0]

# 根据‘/’截断zhuangxiu字段
housedata[['cengshi', 'zhuangxiuchengdu']] = housedata['zhuangxiu'].str.replace('[', '').str.replace(']', '').str.replace('\'','').str.split('/', expand=True)

#根据‘/’截断louceng字段
housedata[['height_location', 'height']] = housedata['louceng'].str.replace('[', '').str.replace(']', '').str.replace('\'','').str.split('共', expand=True)
housedata['height_location'] = housedata['height_location'].str.replace('/', '').str.replace('地下室', '0').str.replace('低楼层', '1').str.replace('中楼层', '2').str.replace('高楼层', '3').str.replace('联排', '4')
#TODO:用apply把为空的字段变成-1
housedata['height'] = housedata['height'].str.replace('共', '').str.replace('层', '').str.replace('/', '')
housedata['height'] = pd.to_numeric(housedata['height'],errors='ignore')

#其他数据去掉爬虫保存格式list中的“['']”内容
housedata['title'] = housedata['title'].str.replace('[', '').str.replace(']', '').str.replace('\'','')
housedata['community_name'] = housedata['community_name'].str.replace('[', '').str.replace(']', '').str.replace('\'','')
housedata['location_info'] = housedata['location_info'].str.replace('[', '').str.replace(']', '').str.replace('\'','')
housedata['location_info_sec'] = housedata['location_info_sec'].str.replace('[', '').str.replace(']', '').str.replace('\'','')

#去除follower字段的‘人关注’
housedata['follower'] = housedata['follower'].str.replace('[', '').str.replace(']', '').str.replace('\'','').str.replace('人关注', '')

#将elevator，elevator_house字段进行统一格式
housedata['elevator'] = housedata['elevator'].str.replace('暂无数据', '0').str.replace('有', '1').str.replace('无','0')
housedata['elevator'] = pd.to_numeric(housedata['elevator'],errors='ignore')

housedata['elevator_house'] = housedata['elevator_house'].str.replace('暂无数据', '零梯零户')
housedata[['elevator_num', 'house_num']] = housedata['elevator_house'].str.replace('户', '').str.replace('梯', ' ').str.split(' ', expand=True)
print(housedata['elevator_num'])
#housedata['elevator_num'] = housedata['elevator_num'].apply(lambda x: cn2an.transform(x))
#housedata['house_num'] = cn2an.cn2an(housedata['house_num'],"smart")

#去除空元素
housedata.dropna(axis=0, subset=['total_price','unit_price','location_info','building_area'], inplace=True)

#去除重复元素
housedata.drop_duplicates(inplace=True)

print(housedata.head())

housedata.to_csv('xmhouse_data_clean.csv')
