package util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisUtil {
  private val conf = new JedisPoolConfig
  conf.setMaxTotal(100)
  conf.setMaxIdle(10)
  conf.setMinIdle(10)
  conf.setBlockWhenExhausted(true)
  conf.setMaxWaitMillis(10000)
  conf.setTestOnBorrow(true)
  conf.setTestOnReturn(true)
    val pool = new JedisPool(conf,"hadoop002",6379)
  def getClient=pool.getResource

}
