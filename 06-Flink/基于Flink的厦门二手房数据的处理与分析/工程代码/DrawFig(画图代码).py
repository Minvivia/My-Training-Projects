import pandas as pd
from pyecharts.charts import WordCloud
from pyecharts import options as opts
from pyecharts.charts import Bar
from pyecharts.charts import Pie
from pyecharts.charts import Line
from pyecharts.charts import Page,Scatter
from pyecharts.charts import HeatMap
import matplotlib.pyplot as plt
import plotly.express as px
from pyecharts.commons.utils import JsCode

def draw_wordcloud(csv_file_path, output_html_path, with_title=True):
    # 读取CSV文件，并手动添加列名
    df = pd.read_csv(csv_file_path, header=None, names=['word', 'frequency'], encoding='utf-8')

    # 创建词云对象
    wordcloud = WordCloud()

    # 添加词语和频率数据
    wordcloud.add(
        series_name="Word Frequency",
        data_pair=[(word, freq) for word, freq in zip(df['word'], df['frequency'])],
        word_size_range=[10, 100],
        shape='circle'
    )

    # 设置全局选项
    if with_title:
        wordcloud.set_global_opts(
            title_opts=opts.TitleOpts(title="厦门二手房房源宣传语词频统计"),
            tooltip_opts=opts.TooltipOpts(is_show=True)
        )
    else:
        wordcloud.set_global_opts(
            tooltip_opts=opts.TooltipOpts(is_show=True)
        )

    # 渲染词云并保存为HTML文件
    wordcloud.render(output_html_path)

def draw_bar_chart(csv_file_path, output_html_path, top_n=10):
    # 读取CSV文件，并手动添加列名
    df = pd.read_csv(csv_file_path, header=None, names=['word', 'frequency'], encoding='utf-8')

    # 按频率降序排序并取前top_n个词
    df_sorted = df.sort_values(by='frequency', ascending=False).head(top_n)

    # 创建柱状图对象
    bar = Bar()

    # 添加词语和频率数据
    bar.add_xaxis(df_sorted['word'].tolist())
    bar.add_yaxis("出现次数", df_sorted['frequency'].tolist())

    # 设置全局选项
    bar.set_global_opts(
        title_opts=opts.TitleOpts(title="高频词柱状图", title_textstyle_opts=opts.TextStyleOpts(font_weight='bold')),  # 标题加粗
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=45, font_weight='bold')),  # x轴标签加粗
        yaxis_opts=opts.AxisOpts(name="频率", name_textstyle_opts=opts.TextStyleOpts(font_weight='bold')),  # y轴名称加粗
        tooltip_opts=opts.TooltipOpts(is_show=True)
    )

    # 渲染柱状图并保存为HTML文件
    bar.render(output_html_path)

def draw_pie_chart(csv_file_path, output_html_path):
    # 读取CSV文件，并手动添加列名
    df = pd.read_csv(csv_file_path, header=None, names=['location', 'house_count'], encoding='utf-8')

    # 创建饼状图对象
    pie = Pie()

    # 添加区与房源数量数据
    pie.add(
        series_name="House Count by Location",
        data_pair=[(location, house_count) for location, house_count in zip(df['location'], df['house_count'])],
        radius=["30%", "75%"]  # 内外半径
    )

    # 设置全局选项
    pie.set_global_opts(
        title_opts=opts.TitleOpts(title="区与房源数量关系", title_textstyle_opts=opts.TextStyleOpts(font_weight='bold')),  # 标题加粗
        legend_opts=opts.LegendOpts(is_show=True, textstyle_opts=opts.TextStyleOpts(font_weight='bold')),  # 图例加粗
        tooltip_opts=opts.TooltipOpts(is_show=True)
    )

    # 设置系列选项
    pie.set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)"))

    # 渲染饼状图并保存为HTML文件
    pie.render(output_html_path)

def draw_avgElevatorNumByYear_chart(csv_file_path, output_html_path):
    # 读取CSV文件，并手动添加列名
    df = pd.read_csv(csv_file_path, header=None, names=['year', 'avg_elevator_num', 'house_totalnum'])

    # 过滤掉值为 -1 和 1949 的行
    df = df[(df['year'] != -1) & (df['year'] != 1949)]
    df['avg_elevator_num'] = df['avg_elevator_num'].round(1)

    # 创建柱状图对象
    bar = Bar()

    # 添加柱状图数据
    bar.add_xaxis(df['year'].tolist())
    bar.add_yaxis("平均电梯数", df['avg_elevator_num'].tolist(), label_opts=opts.LabelOpts(position="top"))
    bar.add_yaxis("房源总数", df['house_totalnum'].tolist(), label_opts=opts.LabelOpts(position="top"))

    # 设置全局选项
    bar.set_global_opts(
        title_opts=opts.TitleOpts(title="建筑年份与平均梯户比关系", title_textstyle_opts=opts.TextStyleOpts(font_weight='bold')),  # 加粗
        xaxis_opts=opts.AxisOpts(name="建筑年份", axislabel_opts=opts.LabelOpts(font_weight='bold')),  # 加粗
        yaxis_opts=opts.AxisOpts(name="数量", axislabel_opts=opts.LabelOpts(font_weight='bold')),  # 加粗
        tooltip_opts=opts.TooltipOpts(is_show=True)
    )

    # 渲染柱状图并保存为HTML文件
    bar.render(output_html_path)


def draw_avg_unit_price_chart(csv_file_path, output_html_path, page_width="3000px"):

    # 读取CSV文件，并手动添加列名
    df = pd.read_csv(csv_file_path, header=None, names=['community_name', 'avg_unit_price'])

    # 对数据按照房屋均价降序排序
    df_sorted = df.sort_values(by='avg_unit_price', ascending=False)

    # 单位换算：将房屋均价转换为万元
    df_sorted['avg_unit_price'] = df_sorted['avg_unit_price'] / 10000

    # 仅选择前30个小区进行显示
    df_top30 = df_sorted.head(20)

    # 创建柱状图对象
    bar = Bar()

    # 添加柱状图数据
    bar.add_xaxis(df_top30['community_name'].tolist())
    bar.add_yaxis("房屋均价（万元）", df_top30['avg_unit_price'].round(1).tolist(), 
                  label_opts=opts.LabelOpts(position="top", formatter="{c}"),
                  itemstyle_opts=opts.ItemStyleOpts(color=JsCode("""
                        function(params) {
                            var colorList = [
                                        '#FF0000', '#FF7F50', '#FF6347', '#FFD700', '#ADFF2F',
                                        '#00FF00', '#32CD32', '#00FA9A', '#00FFFF', '#00CED1',
                                        '#FF1493', '#DB7093', '#FFC0CB', '#FFA07A', '#FF8C00',
                                        '#8B0000', '#800000', '#B22222', '#DC143C', '#FF69B4',
                            ];
                            return colorList[params.dataIndex];
                        }
                    """)
                 )
                 )

    # 设置全局选项
    bar.set_global_opts(
        title_opts=opts.TitleOpts(title="小区房屋均价柱状图"),
        xaxis_opts=opts.AxisOpts(
            name="小区名称",
            axislabel_opts=opts.LabelOpts(interval=0, rotate=-25, font_weight='bold'),  # 调整柱子之间的间距
            name_textstyle_opts=opts.TextStyleOpts(font_weight='bold')  # 加粗
        ),
        yaxis_opts=opts.AxisOpts(name="房屋均价（万元）", axislabel_opts=opts.LabelOpts(font_weight='bold')),  # 加粗
        tooltip_opts=opts.TooltipOpts(is_show=True, formatter="{c} 万元"),
    )

    # 渲染柱状图
    html_content = bar.render_embed()

    # 将样式应用到 HTML 文件中
    html_content_with_style = f"""
    <html>
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>小区房屋均价柱状图</title>
    <style>
    .container {{
        width: {page_width};
        margin: 0 auto;
    }}
    .axis-label {{
        font-weight: bold; /* 加粗 */
    }}
    .axis-name {{
        font-weight: bold; /* 加粗 */
    }}
    </style>
    </head>
    <body>
    <div class="container">
    {html_content}
    </div>
    </body>
    </html>
    """

    # 将带样式的 HTML 内容写入文件
    with open(output_html_path, "w", encoding="utf-8") as file:
        file.write(html_content_with_style)

def draw_popularity_bubble_chart(csv_file_path, output_html_path, page_width, page_height):
    # 读取CSV文件，并手动添加列名
    df = pd.read_csv(csv_file_path, header=None, names=['community_name', 'popularity', 'total_house_count'])

    # 对数据按照受欢迎程度降序排序，并选择前15个小区
    df_sorted = df.sort_values(by='popularity', ascending=False).head(15)

    # 设置颜色列表
    colors = [
        '#FF0000', '#FF7F50', '#FF6347', '#FFD700', '#ADFF2F',
        '#00FF00', '#32CD32', '#00FA9A', '#00FFFF', '#00CED1',
        '#4682B4', '#0000FF', '#1E90FF', '#800080', '#FF00FF',
        '#FF1493', '#DB7093', '#FFC0CB', '#FFA07A', '#FF8C00',
        '#8B0000', '#800000', '#B22222', '#DC143C', '#FF69B4',
        '#FFD700', '#FFFF00', '#ADFF2F', '#7FFF00', '#7CFC00'
    ]

    # 创建气泡图对象
    scatter = Scatter()

    # 添加气泡图数据
    scatter.add_xaxis(df_sorted['community_name'].tolist())
    scatter.add_yaxis(
        series_name="受欢迎程度",
        y_axis=df_sorted['popularity'].tolist(),
        symbol_size=15,  # 固定气泡大小
        label_opts=opts.LabelOpts(is_show=False),
        itemstyle_opts=opts.ItemStyleOpts(color=JsCode("""
            function (params) {
                var colorList = [
                    '#FF0000', '#FF7F50', '#FF6347', '#FFD700', '#ADFF2F',
                    '#00FF00', '#32CD32', '#00FA9A', '#00FFFF', '#00CED1',
                    '#4682B4', '#0000FF', '#1E90FF', '#800080', '#FF00FF',
                    '#FF1493', '#DB7093', '#FFC0CB', '#FFA07A', '#FF8C00',
                    '#8B0000', '#800000', '#B22222', '#DC143C', '#FF69B4',
                    '#FFD700', '#FFFF00', '#ADFF2F', '#7FFF00', '#7CFC00'
                ];
                return colorList[params.dataIndex % colorList.length];
            }
        """))
    )

    # 设置全局选项
    scatter.set_global_opts(
        title_opts=opts.TitleOpts(title="小区受欢迎程度气泡图", title_textstyle_opts=opts.TextStyleOpts(font_weight='bold')),
        xaxis_opts=opts.AxisOpts(
            name="小区名称",
            axislabel_opts=opts.LabelOpts(interval=0, rotate=-20, font_weight='bold'),
            name_textstyle_opts=opts.TextStyleOpts(font_weight='bold'),
            splitline_opts=opts.SplitLineOpts(is_show=True, linestyle_opts=opts.LineStyleOpts(color="#FFFFFF"))
        ),
        yaxis_opts=opts.AxisOpts(
            name="受欢迎程度",
            axislabel_opts=opts.LabelOpts(font_weight='bold'),
            name_textstyle_opts=opts.TextStyleOpts(font_weight='bold'),
            splitline_opts=opts.SplitLineOpts(is_show=True, linestyle_opts=opts.LineStyleOpts(color="#FFFFFF"))
        ),
        tooltip_opts=opts.TooltipOpts(is_show=False)
    )
    
    # 渲染气泡图
    html_content = scatter.render_embed()

    ## 重新生成 legend_html 部分
    legend_html = """
        <h4 style="margin: 0; text-align: center;">小区名称及颜色</h4>
        <ul style="list-style: none; padding: 0; margin: 0;">
    """

    for index, row in df_sorted.iterrows():
        legend_html += f"<li><span style='background-color:{colors[index % len(colors)]}; width: 20px; height: 20px; display: inline-block;'></span> {row['community_name']}</li>"

    legend_html += """
        </ul>
    """
    scatter_width = "800px"
   # 将带样式的 HTML 内容写入文件
    html_content_with_style = f"""
    <html>
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>小区受欢迎程度气泡图</title>
    <style>
    .container {{
        width: {page_width};
        height: {page_height};
        margin: 0 auto;
        position: relative;
        display: flex; /* 使用 flex 布局 */
    }}
    .chart-container {{
        background: rgba(173, 216, 230, 0.9);
        padding: 20px;
        border-radius: 10px;
        border: 1px solid black; /* 添加边框属性 */
        width: {scatter_width};
        height: {page_height};
        float: left; /* 左浮动 */
    }}
    .legend-container {{
        background: rgba(173, 216, 230, 0.9); 
        border-radius: 5px;
        padding: 10px;
        height: {page_height};
    }}
    </style>
    </head>
    <body>
    <div class="container">
        {html_content}
        <div class="legend-container">
            {legend_html}
        </div>
    </div>
    </body>
    </html>
    """

    # 将带样式的 HTML 内容写入文件
    with open(output_html_path, "w", encoding="utf-8") as file:
        file.write(html_content_with_style)




if __name__ == "__main__":

    draw_wordcloud(
        csv_file_path='C:\\Users\\13048\\Desktop\\基于Flink的厦门二手房数据的处理与分析\\数据集\\outputData(数据分析结果)\\sortedWordFrequency.csv',  # 输入CSV文件路径
        output_html_path='C:/Users/13048/Desktop/datadata/data/sortedWordFrequency_wordcloud.html',  # 输出HTML文件路径
        with_title=True  # 设置为 False 则不显示标题
    )
    draw_bar_chart(
        csv_file_path='C:\\Users\\13048\\Desktop\\基于Flink的厦门二手房数据的处理与分析\\数据集\\outputData(数据分析结果)\\sortedWordFrequency.csv',  # 输入CSV文件路径
        output_html_path='C:/Users/13048/Desktop/datadata/data/sortedWordFrequency_chart.html',  # 输出HTML文件路径
        top_n=30  # 显示前30个高频词
    )
    draw_pie_chart(
        csv_file_path='C:\\Users\\13048\\Desktop\\基于Flink的厦门二手房数据的处理与分析\\数据集\\outputData(数据分析结果)\\TotalHouseOfLocation.csv',  # 输入CSV文件路径
        output_html_path='C:/Users/13048/Desktop/datadata/data/TotalHouseOfLocation_chart.html'  # 输出HTML文件路径
    )
    draw_avgElevatorNumByYear_chart(
        csv_file_path='C:\\Users\\13048\\Desktop\\基于Flink的厦门二手房数据的处理与分析\\数据集\\outputData(数据分析结果)\\avgElevatorNumByYear.csv',  # 输入CSV文件路径
        output_html_path='C:/Users/13048/Desktop/datadata/data/avgElevatorNumByYear_chart.html'  # 输出HTML文件路径
    )
    
    draw_avg_unit_price_chart(
       csv_file_path = 'C:\\Users\\13048\\Desktop\\基于Flink的厦门二手房数据的处理与分析\\数据集\\outputData(数据分析结果)\\avgUnitPriceStream.csv',
       output_html_path = 'C:/Users/13048/Desktop/datadata/data/avgUnitPrice_chart.html',
       page_width="3000px"
    )
    
    draw_popularity_bubble_chart(
       csv_file_path = 'C:\\Users\\13048\\Desktop\\基于Flink的厦门二手房数据的处理与分析\\数据集\\outputData(数据分析结果)\\sortedPopularity.csv',
       output_html_path =  'C:/Users/13048/Desktop/datadata/data/sortedPopularity_chart.html',
       page_width="1500px",
       page_height ="541.32spx"
    )

