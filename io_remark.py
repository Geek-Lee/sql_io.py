import pandas as pd
import sys
import re

def sql_cols(df, usage="sql"):
    cols = tuple(df.columns)
    if usage == "sql":
        cols_str = str(cols).replace("'", "`")
        if len(df.columns) == 1:
            cols_str = cols_str[:-2] + ")"
            # to process dataframe with only one column
            #只用一列处理dataframe
            #>>> cols = tuple(["a","b","c"])
            #>>> cols
            #('a', 'b', 'c')
            #>>> str(cols)
            #"('a', 'b', 'c')"
            #>>> cols_str = str(cols).replace("'", "`")
            #>>> cols_str
            #'(`a`, `b`, `c`)'
            #>>> cols1 = tuple(["a"])
            #>>> cols_str1 = str(cols1).replace("'", "`")
            #>>> cols_str1
            #'(`a`,)'
            #>>> cols_str1[:-2]
            #'(`a`'
            #>>> 
        return cols_str
    elif usage == "format":
        base = "'%%(%s)s'" % cols[0]
        #输出为%字符串s
        for col in cols[1:]:
            base += ", '%%(%s)s'" % col
        #后面的加上，
        return base
        #base为【%字符串s，%字符串s，%字符串s】
    elif usage == "values":
        base = "%s=VALUES(%s)" % (cols[0], cols[0])
        for col in cols[1:]:
            base += ", `%s`=VALUES(`%s`)" % (col, col)
        return base


def to_sql(tb_name, conn, dataframe, type="update", chunksize=2000):
    """
    Dummy of pandas.to_sql, support "REPLACE INTO ..." and "INSERT ... ON DUPLICATE KEY UPDATE (keys) VALUES (values)"
    SQL statement.

    Args:
        tb_name: str
            Table to insert get_data;
        conn:
            DBAPI Instance
        dataframe: pandas.DataFrame
            Dataframe instance
        type: str, optional {"update", "replace", "ignore"}, default "update"
            Specified the way to update get_data. If "update", then `conn` will execute "INSERT ... ON DUPLICATE UPDATE ..."
            SQL statement, else if "replace" chosen, then "REPLACE ..." SQL statement will be executed; else if "ignore" chosen,
            then "INSERT IGNORE ..." will be excuted;
        chunksize: int
            Size of records to be inserted each time;
        **kwargs:

    Returns:
        None
    """

    df = dataframe.copy()
    #复制dataframe
    df = df.fillna("None")
    #填充None
    df = df.applymap(lambda x: re.sub('([\'\"%\\\])', '\\\\\g<1>', str(x)))
    #对df中的元素使用applymap方法：正则替换字符串x
    cols_str = sql_cols(df)
    #dataframe中的columns变成'(`a`, `b`, `c`)'形式
    for i in range(0, len(df), chunksize):
        #len(df)等于columns个数，chunksize为2000
        # print("chunk-{no}, size-{size}".format(no=str(i/chunksize), size=chunksize))
        df_tmp = df[i: i + chunksize]
        #df中的index为i:i+chunksize的部分
        if type == "replace":
            sql_base = "REPLACE INTO `{tb_name}` {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )
            #插入tb_name表中columns
            sql_val = sql_cols(df_tmp, "format")
            #>>> cols = tuple(["a","b","c"])
            #>>> base = "'%%(%s)s'" % cols[0]
            #>>> base
            #"'%(a)s'"
            #>>> for col in cols[1:]:
            #        base += ", '%%(%s)s'" % col
            #>>> base
            #"'%(a)s', '%(b)s', '%(c)s'"
            #>>> 
            vals = tuple([sql_val % x for x in df_tmp.to_dict("records")])
            #>>> '%(class)s' % {'class': 11, 'name': 1, 'price': 111}
            #'11'
            #>>> "'%(class)s','%(name)s','%(price)s'" % {'class': 11, 'name': 1, 'price': 111}
            #"'11','1','111'"
            #映射出值
            
            sql_vals = "VALUES ({x})".format(x=vals[0])
            for i in range(1, len(vals)):
                sql_vals += ", ({x})".format(x=vals[i])
            sql_vals = sql_vals.replace("'None'", "NULL")

            sql_main = sql_base + sql_vals
            #之前的准备都是为了组成一个sql语句
        elif type == "update":
            sql_base = "INSERT INTO `{tb_name}` {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )
            sql_val = sql_cols(df_tmp, "format")
            vals = tuple([sql_val % x for x in df_tmp.to_dict("records")])
            sql_vals = "VALUES ({x})".format(x=vals[0])
    
        elif type == "ignore":
            sql_base = "INSERT IGNORE INTO `{tb_name}` {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )
            sql_val = sql_cols(df_tmp, "format")
            vals = tuple([sql_val % x for x in df_tmp.to_dict("records")])
            sql_vals = "VALUES ({x})".format(x=vals[0])
            for i in range(1, len(vals)):
                sql_vals += ", ({x})".format(x=vals[i])
            sql_vals = sql_vals.replace("'None'", "NULL")

            sql_main = sql_base + sql_vals

        for i in range(1, len(vals)):
                sql_vals += ", ({x})".format(x=vals[i])
            sql_vals = sql_vals.replace("'None'", "NULL")

            sql_update = "ON DUPLICATE KEY UPDATE {0}".format(
                sql_cols(df_tmp, "values")
            )

            sql_main = sql_base + sql_vals + sql_update

        elif type == "ignore":
            sql_base = "INSERT IGNORE INTO `{tb_name}` {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )
            sql_val = sql_cols(df_tmp, "format")
            vals = tuple([sql_val % x for x in df_tmp.to_dict("records")])
            sql_vals = "VALUES ({x})".format(x=vals[0])
            for i in range(1, len(vals)):
                sql_vals += ", ({x})".format(x=vals[i])
            sql_vals = sql_vals.replace("'None'", "NULL")

            sql_main = sql_base + sql_vals

        if sys.version_info.major == 2:
            sql_main = sql_main.replace("u`", "`")
        # sql_main = sql_main.replace("%", "%%")
        conn.execute(sql_main)

