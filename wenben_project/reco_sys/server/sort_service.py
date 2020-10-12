#from server import SORT_SPARK
#from pyspark.ml.linalg import DenseVector
#from pyspark.ml.classification import LogisticRegressionModel
#import pandas as pd
#import numpy as np
#from datetime import datetime
#import logging
#
#logger = logging.getLogger("recommend")
#
#
#def lr_sort_service(reco_set, temp, hbu):
#    """
#    排序返回推荐文章
#    :param reco_set:召回合并过滤后的结果
#    :param temp: 参数
#    :param hbu: Hbase工具
#    :return:
#    """
#    # 排序
#    # 1、读取用户特征中心特征
#    try:
#        user_feature = eval(hbu.get_table_row('ctr_feature_user',
#                                              '{}'.format(temp.user_id).encode(),
#                                              'channel:{}'.format(temp.channel_id).encode()))
#        logger.info("{} INFO get user user_id:{} channel:{} profile data".format(
#            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
#    except Exception as e:
#        user_feature = []
#
#    if user_feature:
#        # 2、读取文章特征中心特征
#        result = []
#
#        for article_id in reco_set:
#            try:
#                article_feature = eval(hbu.get_table_row('ctr_feature_article',
#                                                         '{}'.format(article_id).encode(),
#                                                         'article:{}'.format(article_id).encode()))
#            except Exception as e:
#
#                article_feature = [0.0] * 111
#            f = []
#            # 第一个channel_id
#            f.extend([article_feature[0]])
#            # 第二个article_vector
#            f.extend(article_feature[11:])
#            # 第三个用户权重特征
#            f.extend(user_feature)
#            # 第四个文章权重特征
#            f.extend(article_feature[1:11])
#            vector = DenseVector(f)
#            result.append([temp.user_id, article_id, vector])
#
#        # 4、预测并进行排序是筛选
#        df = pd.DataFrame(result, columns=["user_id", "article_id", "features"])
#        test = SORT_SPARK.createDataFrame(df)
#
#        # 加载逻辑回归模型
#        model = LogisticRegressionModel.load("hdfs://hadoop-master:9000/headlines/models/logistic_ctr_model.obj")
#        predict = model.transform(test)
#
#        def vector_to_double(row):
#            return float(row.article_id), float(row.probability[1])
#
#        res = predict.select(['article_id', 'probability']).rdd.map(vector_to_double).toDF(
#            ['article_id', 'probability']).sort('probability', ascending=False)
#        article_list = [i.article_id for i in res.collect()]
#        logger.info("{} INFO sorting user_id:{} recommend article".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
#                                                                          temp.user_id))
#        # 排序后，只将排名在前100个文章ID返回给用户推荐N个
#        if len(article_list) > 100:
#            article_list = article_list[:100]
#        reco_set = list(map(int, article_list))
#
#    return reco_set


import tensorflow as tf
from grpc.beta import implementations
from tensorflow_serving.apis import prediction_service_pb2_grpc
from tensorflow_serving.apis import classification_pb2
import os
import sys
import grpc
import numpy as np
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from server.utils import HBaseUtils
from server import pool


def wdl_sort_service(reco_set, temp, hbu):
#def wdl_sort_service():
    """
    wide&deep进行排序预测
    :param reco_set:
    :param temp:
    :param hbu:
    :return:
    """
    #hbu = HBaseUtils(pool)
    # 排序
    # 1、读取用户特征中心特征
    try:
        user_feature = eval(hbu.get_table_row('ctr_feature_user',
                                              #'{}'.format(1105093883106164736).encode(),
                                              #'channel:{}'.format(18).encode()))
                                              '{}'.format(temp.user_id).encode(),
                                              'channel:{}'.format(temp.channel_id).encode()))
        # logger.info("{} INFO get user user_id:{} channel:{} profile data".format(
        #     datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
        print(user_feature)
    except Exception as e:
        user_feature = []
    if user_feature:
        # 2、读取文章特征中心特征
        result = []

        # examples
        examples = []
        #for article_id in [108245, 108246, 108242, 108240]:
        for article_id in reco_set:
            try:
                article_feature = eval(hbu.get_table_row('ctr_feature_article',
                                                         '{}'.format(article_id).encode(),
                                                         'article:{}'.format(article_id).encode()))
            except Exception as e:
                article_feature = [0.0] * 111

            # article_feature结构： [channel, 10weights, 100vector]

            # 构造每一个文章与用户的example结构,训练样本顺序
            channel_id = int(article_feature[0])

            vector = np.mean(article_feature[11:])

            user_weights = np.mean(user_feature)

            article_weights = np.mean(article_feature[1:11])

            # 封装到example(一次一个样本)
            example = tf.train.Example(features=tf.train.Features(feature={
                "channel_id": tf.train.Feature(int64_list=tf.train.Int64List(value=[channel_id])),
                "vector": tf.train.Feature(float_list=tf.train.FloatList(value=[vector])),
                'user_weights': tf.train.Feature(float_list=tf.train.FloatList(value=[user_weights])),
                'article_weights': tf.train.Feature(float_list=tf.train.FloatList(value=[article_weights])),
            }))

            examples.append(example)

        # 所有的样本，放入一个列表中
        # 调用tensorflow serving的模型服务
        with grpc.insecure_channel("127.0.0.1:8500") as channel:
            stub = prediction_service_pb2_grpc.PredictionServiceStub(channel)

            # 构造请求
            # 模型名称，将要预测的example样本列表
            request = classification_pb2.ClassificationRequest()
            request.model_spec.name = 'wdl'
            request.input.example_list.examples.extend(examples)

            # 发送请求
            response = stub.Classify(request, 10.0)
            print(response)

    #return 处理之后的response

if __name__ == '__main__':
    wdl_sort_service()
