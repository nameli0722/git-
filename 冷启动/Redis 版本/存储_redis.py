# 新文章存储
# ZADD ZRANGE
# ZADD key score member [[score member] [score member] ...]
# ZRANGE page_rank 0 -1
client.zadd("ch:{}:new".format(channel_id), {article_id: time.time()})

# 热门文章存储
# ZINCRBY key increment member
# ZSCORE
# 为有序集 key 的成员 member 的 score 值加上增量 increment 。
client.zincrby("ch:{}:hot".format(row['channelId']), 1, row['param']['articleId'])

# ZREVRANGE key start stop [WITHSCORES]
client.zrevrange(ch: {}:new, 0, -1)