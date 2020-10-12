from offline.update_article import UpdateArticle
from offline.update_user import UpdateUserProfile
from offline.update_ctr_feature import FeaturePlatform
from settings import logging as lg
lg.create_logger()
from offline.update_recall import UpdateRecall

def update_recall():
    """
    更新用户的召回集
    :return:
    """
    udp = UpdateRecall(200)
    udp.update_als_recall()
    udp.update_content_recall()

def update_article_profile():
    """
    更新文章画像
    :return:
    """
    ua = UpdateArticle()
    sentence_df = ua.merge_article_data()
    if sentence_df.rdd.collect():
      text_rank_res = ua.generate_article_label(sentence_df)
      article_profile = ua.get_article_profile(text_rank_res)
      ua.compute_article_similar(article_profile)


def update_user_profile():
    """
    更新用户画像
    """
    uup = UpdateUserProfile()
    if uup.update_user_action_basic():
        uup.update_user_label()
        uup.update_user_info()

def update_ctr_feature():
    """
    定时更新用户、文章特征
    :return:
    """
    fp = FeaturePlatform()
    fp.update_user_ctr_feature_to_hbase()
    fp.update_article_ctr_feature_to_hbase()