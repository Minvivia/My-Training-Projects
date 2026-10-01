import requests
from lxml import etree
import pandas as pd
import time
import random


def parse_url(url):
    """输入链接，返回解析后的html"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive"
    }
    try:
        response = requests.get(url=url, headers=headers, timeout=15)
        response.raise_for_status()  # 检查HTTP错误
        content = response.content.decode('utf-8', 'ignore')
        return etree.HTML(content)
    except Exception as e:
        print(f"请求URL失败: {url}, 错误: {e}")
        return None


def get_base_info(page_url):
    """获取基础信息"""
    html_content = parse_url(page_url)
    if html_content is None:
        print(f"无法解析页面: {page_url}")
        return []

    print("page url", page_url)
    base_infos = []

    # 使用更通用的XPath来获取所有列表项
    listings = html_content.xpath('//*[@id="content"]/div[1]/ul/li')
    if not listings:
        print(f"在页面 {page_url} 上没有找到房源列表")
        return []

    for listing in listings:
        info = {}

        # 标题
        title_elem = listing.xpath('./div[1]/div[1]/a/text()')
        info['title'] = title_elem[0] if title_elem else ''

        # URL
        url_elem = listing.xpath('./a/@href')
        info['url'] = url_elem[0] if url_elem else ''

        # 总价
        total_price_elem = listing.xpath('./div[1]/div[6]/div[1]/span/text()')
        info['total_price'] = total_price_elem[0] if total_price_elem else ''

        # 均价
        unit_price_elem = listing.xpath('./div[1]/div[6]/div[2]/span/text()')
        info['unit_price'] = unit_price_elem[0] if unit_price_elem else ''

        # 关注者
        follower_elem = listing.xpath('./div[1]/div[4]/text()')
        if follower_elem:
            # 修复原代码中的错误：使用 [0] 索引
            if info['url'] and 'goodhouse' not in info['url']:
                info['follower'] = follower_elem[0].split('/')[0].strip()
            else:
                info['follower'] = follower_elem[0]
        else:
            info['follower'] = ''

        base_infos.append(info)

    return base_infos


def get_extra_info_plus(info):
    """进入详情页获取更多信息"""
    if not info.get('url'):
        print("没有URL，跳过详情页爬取")
        return info

    print("info url", info['url'])
    html_content = parse_url(info['url'])
    if html_content is None:
        print(f"无法解析详情页: {info['url']}")
        return info

    # 使用字典存储XPath和对应的字段名，便于维护
    xpath_mappings = {
        'huxing': '//*[@id="introduction"]/div/div/div[1]/div[2]/ul/li[1]/text()',
        'louceng': '/html/body/div[5]/div[2]/div[4]/div[1]/div[2]/text()',
        'chaoxiang': '/html/body/div[5]/div[2]/div[4]/div[2]/div[1]/text()',
        'zhuangxiu': '/html/body/div[5]/div[2]/div[4]/div[2]/div[2]/text()',
        'building_area': '/html/body/div[5]/div[2]/div[4]/div[3]/div[1]/text()',
        'build_year': '/html/body/div[5]/div[2]/div[4]/div[3]/div[2]/text()',
        'community_name': '/html/body/div[5]/div[2]/div[5]/div[1]/a[1]/text()',
        'location_info': '/html/body/div[5]/div[2]/div[5]/div[2]/span[2]/a[1]/text()',
        'location_info_sec': '/html/body/div[5]/div[2]/div[5]/div[2]/span[2]/a[2]/text()',
        'elevator': '//*[@id="introduction"]/div/div/div[1]/div[2]/ul/li[11]/text()',
        'elevator_house': '//*[@id="introduction"]/div/div/div[1]/div[2]/ul/li[10]/text()',
        'listing_day': '//*[@id="introduction"]/div/div/div[2]/div[2]/ul/li[1]/span[2]/text()',
        'last_trade': '//*[@id="introduction"]/div/div/div[2]/div[2]/ul/li[3]/span[2]/text()'
    }

    for field, xpath in xpath_mappings.items():
        try:
            result = html_content.xpath(xpath)
            # 处理特殊字段
            if field in ['huxing', 'elevator', 'elevator_house'] and result:
                info[field] = result[1].strip() if len(result) > 1 else result[0].strip()
            elif result:
                info[field] = result[0].strip() if isinstance(result[0], str) else result[0]
            else:
                info[field] = ''
        except Exception as e:
            print(f"解析字段 {field} 失败: {e}")
            info[field] = ''

    return info


# 主程序
if __name__ == "__main__":
    base_url = 'https://xm.lianjia.com/ershoufang/'
    infos = []
    max_pages = 5  # 测试时减少页面数量

    for i in range(1, max_pages + 1):
        time.sleep(random.randint(5, 10))  # 更长的随机等待时间

        if i == 1:
            page_url = base_url
        else:
            page_url = f"{base_url}pg{i}/"

        results = get_base_info(page_url)
        if results:
            infos.extend(results)
            print(f'爬取页面 {i} 成功，获取 {len(results)} 条基础信息')
        else:
            print(f'爬取页面 {i} 失败或没有数据')
            break

    # 保存基础信息作为备份
    if infos:
        pd.DataFrame(infos).to_csv('base_info_backup.csv', index=False)

    # 获取额外信息
    total_count = len(infos)
    for idx, info in enumerate(infos):
        time.sleep(random.randint(3, 8))
        infos[idx] = get_extra_info_plus(info)

        # 检查是否触发人机验证
        if not info.get('huxing') and not info.get('location_info') and 'goodhouse' not in info.get('url', ''):
            print(f'爬取第 {idx} 条信息失败, 可能触发人机验证!')
            print(info['url'])
            # 保存当前进度
            pd.DataFrame(infos).to_csv('data_temp.csv', index=False)
            break
        else:
            print(f"爬取进度: {idx + 1}/{total_count}, 标题: {info.get('title', '')}")

    # 最终保存
    pd.DataFrame(infos).to_csv('lianjia_data_final.csv', index=False)
    print(f"爬取完成，共获取 {len(infos)} 条数据")