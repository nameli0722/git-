import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from concurrent import futures
from abtest import user_reco_pb2
from abtest import user_reco_pb2_grpc
from settings.default import DefaultConfig, RAParam
from server.reco_center import RecoCenter
import grpc
import time
import json
import hashlib
import settings.logging as lg


def feed_recommend(user_id, channel_id, article_num, time_stamp):
    """
    ABtest的过滤逻辑，对于用户的分流，分配不同算法去推荐
    :param user_id:
    :param channel_id:
    :param article_num:
    :param time_stamp:
    :return:
    """

    class TempParam(object):
        user_id = -10
        channel_id = -10
        article_num = -10
        time_stamp = -10
        algo = ""

    temp = TempParam()
    temp.user_id = user_id
    temp.channel_id = channel_id
    temp.article_num = article_num
    # 请求的时间戳大小
    temp.time_stamp = time_stamp

    # 进行用户的分流
    if temp.user_id == "":
        return add_track([], temp)

    # ID为正常
    code = hashlib.md5(temp.user_id.encode()).hexdigest()[:1]
    if code in RAParam.BYPASS[0]['Bucket']:
        temp.algo = RAParam.BYPASS[0]['Strategy']
    else:
        temp.algo = RAParam.BYPASS[1]['Strategy']

    #_track = add_track([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], temp)
    _track = RecoCenter().feed_recommend_time_stamp_logic(temp)

    return _track

def add_track(res, temp):
    """
    封装埋点参数
    :param res: 推荐文章id列表
    :param cb: 合并参数
    :param rpc_param: rpc参数
    :return: 埋点参数
        文章列表参数
        单文章参数
    """
    # 添加埋点参数
    track = {}

    # 准备曝光参数
    # 全部字符串形式提供，在hive端不会解析问题
    _exposure = {"action": "exposure", "userId": temp.user_id, "articleId": json.dumps(res),
                 "algorithmCombine": temp.algo}

    track['param'] = json.dumps(_exposure)
    track['recommends'] = []

    # 准备其它点击参数
    for _id in res:
        # 构造字典
        _dic = {}
        _dic['article_id'] = _id
        _dic['param'] = {}

        # 准备click参数
        _p = {"action": "click", "userId": temp.user_id, "articleId": str(_id),
              "algorithmCombine": temp.algo}

        _dic['param']['click'] = json.dumps(_p)
        # 准备collect参数
        _p["action"] = 'collect'
        _dic['param']['collect'] = json.dumps(_p)
        # 准备share参数
        _p["action"] = 'share'
        _dic['param']['share'] = json.dumps(_p)
        # 准备detentionTime参数
        _p["action"] = 'read'
        _dic['param']['read'] = json.dumps(_p)

        track['recommends'].append(_dic)

    track['timestamp'] = temp.time_stamp
    return track

class UserRecommendServicer(user_reco_pb2_grpc.UserRecommendServicer):
    """grpc黑马推荐接口服务端逻辑写
    """

    def user_recommend(self, request, context):

        # 1、接收参数解析封装
        user_id = request.user_id
        channel_id = request.channel_id
        article_num = request.article_num
        time_stamp = request.time_stamp

        # 2、去进行获取用户abtest分流，到推荐中心获取推荐结果，封装参数
        # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        _track = feed_recommend(user_id, channel_id, article_num, time_stamp)

        # # 埋点参数参考：
        # # {
        # #     "param": '{"action": "exposure", "userId": 1, "articleId": [1,2,3,4],  "algorithmCombine": "c1"}',
        # #     "recommends": [
        # #         {"article_id": 1, "param": {"click": "{"action": "click", "userId": "1", "articleId": 1, "algorithmCombine": 'c1'}", "collect": "", "share": "","read":""}},
        # #         {"article_id": 2, "param": {"click": "", "collect": "", "share": "", "read":""}},
        # #         {"article_id": 3, "param": {"click": "", "collect": "", "share": "", "read":""}},
        # #         {"article_id": 4, "param": {"click": "", "collect": "", "share": "", "read":""}}
        # #     ]
        # #     "timestamp": 1546391572
        # # }
        # 3、将参数进行grpc消息体封装，返回

        # 封装parma1
        # [(article_id, params), (article_id, params),(article_id, params),(article_id, params)]
        _reco = []
        for d in _track['recommends']:

            # 封装param2的消息体
            _param2 = user_reco_pb2.param2(click=d['param']['click'],
                                           collect=d['param']['collect'],
                                           share=d['param']['share'],
                                           read=d['param']['read'])

            # 封装param1的消息体
            _param1 = user_reco_pb2.param1(article_id=d['article_id'], params=_param2)
            _reco.append(_param1)

        return user_reco_pb2.Track(exposure=_track['param'], recommends=_reco, time_stamp=_track['timestamp'])


def serve():
    # 创建recommend日志
    lg.create_logger()

    # 多线程服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # 注册本地服务
    user_reco_pb2_grpc.add_UserRecommendServicer_to_server(UserRecommendServicer(), server)
    # 监听端口
    server.add_insecure_port(DefaultConfig.RPC_SERVER)

    # 开始接收请求进行服务
    server.start()
    # 使用 ctrl+c 可以退出服务
    _ONE_DAY_IN_SECONDS = 60 * 60 * 24
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == '__main__':
    # 测试grpc服务
    serve()