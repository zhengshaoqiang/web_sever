#coding:utf-8


if __name__ == '__main__':
    name = "上海销售中心"
    sql = """select * from sale_kpi where firm='{firm}'""".format(firm=name)
    from hive_helper import hivehelper
    t = hivehelper()
    df = t.query_dataframe(sql)
    print df



