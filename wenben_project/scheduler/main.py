import sys
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
sys.path.insert(0, os.path.join(BASE_DIR, 'reco_sys'))
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ProcessPoolExecutor
from scheduler.update import update_article_profile
from scheduler.update import update_user_profile
from scheduler.update import update_recall
from scheduler.update import update_ctr_feature

# 创建scheduler，多进程执行
executors = {
    'default': ProcessPoolExecutor(3)
}

scheduler = BlockingScheduler(executors=executors)

# 添加定时更新任务更新文章画像,每隔一小时更新
scheduler.add_job(update_article_profile, trigger='interval', hours=1)
scheduler.add_job(update_user_profile, trigger='interval', hours=2)
scheduler.add_job(update_recall, trigger='interval', hour=3)
scheduler.add_job(update_ctr_feature, trigger='interval', hours=4)

scheduler.start()