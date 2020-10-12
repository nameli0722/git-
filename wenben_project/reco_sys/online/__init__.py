from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from settings.default import DefaultConfig

# 1、创建spark streaming context conf
conf = SparkConf()
conf.setAll(DefaultConfig.SPARK_ONLINE_CONFIG)
sc = SparkContext(conf=conf)
stream_sc = StreamingContext(sc, 5)

# 2、配置与kafka读取的配置
similar_kafka = {"metadata.broker.list": DefaultConfig.KAFKA_SERVER, "group.id": 'similar'}
SIMILAR_DS = KafkaUtils.createDirectStream(stream_sc, ['click-trace'], similar_kafka)

# 配置hot文章读取配置
kafka_params = {"metadata.broker.list": DefaultConfig.KAFKA_SERVER}
HOT_DS = KafkaUtils.createDirectStream(stream_sc, ['click-trace'], kafka_params)
#
## 配置新文章读取
NEW_ARTICLE_DS = KafkaUtils.createDirectStream(stream_sc, ['new-article'], kafka_params)

