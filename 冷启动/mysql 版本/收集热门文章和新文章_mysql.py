import os
import sys
import random

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR))
from setting import HOST, USER, PASSWORD, DATABASE, NEW_VIDEO_TIME
import pymysql
import datetime

class OnlineRecall(object):

    def __init__(self):
        # 连接mysql数据库
        self.conn = pymysql.connect(host=HOST,
                                    user=USER,
                                    password=PASSWORD,
                                    database=DATABASE)

    def _update_hot_mysql(self):
        """
        收集用户行为，更新热门视频分数
        :return:
        """
        # 使用cursor()方法创建一个游标对象，操作热门和新视频源表
        cursor1 = self.conn.cursor()
        # 创建另一个字典游标，操作热门视频表
        cursor2 = self.conn.cursor(pymysql.cursors.DictCursor)

        # 读取热门视频数据源表
        sql_1 = "select * from hot_new_source"
        cursor1.execute(sql_1)
        data = cursor1.fetchall()
        # 读取热门视频表
        sql_2 = "select * from hot_video"
        cursor2.execute(sql_2)
        data_hot = cursor2.fetchall()
        # 定位hot_video表的视频id和对应的分数，转化为{123:1, }格式，后续可以将字典按分值排序，召回最热门的topk视频
        hot_id = {}
        for i in data_hot:
            hot_id[i["video_id"]] = i["score"]
        # line 格式如下
        # (1, 123, '心雨花露', 'click', datetime.datetime(2020, 10, 10, 11, 49, 12))
        for line in data:
            if line[3] in ("click", "share", "collect"):
                # 该视频已经存在于热门视频表里，分值在原来的基础上加1分
                if line[1] in hot_id.keys():
                    last_score = hot_id[line[1]]     # 该热门视频之前的分数
                    cursor2.execute('update hot_video set score=%s where video_id=%s;', (last_score + 1, line[1]))
                    hot_id[i["video_id"]] += 1
                else:
                    # 没有热门视视频的情况下插入一条新的视频到hot_video表
                    cursor2.execute('insert into hot_video(video_id, score) values(%s, %s);', (line[1], 1))
                    hot_id[line[1]] = 1
        self.conn.commit()
        # 关闭游标和数据库的连接
        cursor1.close()
        cursor2.close()
        self.conn.close()
        # 更新SQL
        # cursor.execute(sql)
        # db.commit()


    def _update_new_mysql(self):
        """
        推荐新视频
        :return:
        """
        # 使用cursor()方法创建一个游标对象，操作操作热门和新视频源表
        cursor1 = self.conn.cursor()
        # 再创建一个游标对象，操作新视频表，负责往内部插入新视频数据
        cursor2 = self.conn.cursor(pymysql.cursors.DictCursor)

        #读取热门和新视频源表里的所有数据
        sql_1 = "select * from hot_new_source"
        cursor1.execute(sql_1)
        data = cursor1.fetchall()

        # 读取新视频表内已经存在的视频id，放在new_id表
        sql_2 = "select * from new_video"
        cursor2.execute(sql_2)
        new_data = cursor2.fetchall()
        # new_id 存放新视频表里所有的视频id
        new_id = []
        for i in new_data:
            new_id.append(i["video_id"])

        # 将新视频写入到new_video表
        # line 格式如下
        # (1, 123, '心雨花露', 'click', datetime.datetime(2020, 10, 10, 11, 49, 12))
        for line in data:
            # 满足三个条件的是添加到新视频表中：大于某个时间段的是新视频、没有行为的视频、新视频表里没有的
            if line[4] >= datetime.datetime.strptime(NEW_VIDEO_TIME, '%Y-%m-%d  %H:%M:%S') and not line[3] \
                    and line[1] not in new_id:
                cursor2.execute('insert into new_video(video_id, createtime) values(%s, %s);', (line[1], line[4]))

        self.conn.commit()
        # 关闭游标和数据库的连接
        cursor1.close()
        cursor2.close()
        self.conn.close()


    def _random_recommend_mysql(self):
        """
        系统上线，没有任何用户行为数据，先随机打散推荐10条
        :return:
        """
        # 使用cursor()方法创建一个游标对象，操作全部视频表all_video
        cursor1 = self.conn.cursor()
        # 再创建一个游标对象，操作随机推荐表，负责往内部插入随机视频数据
        cursor2 = self.conn.cursor()

        # 读取全部视频表
        sql_1 = "select * from all_video"
        cursor1.execute(sql_1)
        data = list(cursor1.fetchall())   # 转换成列表，便于随机打散
        random.shuffle(data)              #后续需要考虑分类情况，确保推荐的内容不要都在同一类别

        for line in data[:10]:
            cursor2.execute('insert into random_recommend(video_id, createtime) values(%s, %s);', (line[0], line[1]))

        self.conn.commit()
        # 关闭游标和数据库的连接
        cursor1.close()
        cursor2.close()
        self.conn.close()


if __name__ == '__main__':
    ore = OnlineRecall()
    # ore._update_hot_mysql()
    # ore._update_new_mysql()
    # ore._random_recommend_mysql()