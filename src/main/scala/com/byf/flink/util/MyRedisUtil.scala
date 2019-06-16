package com.byf.flink.util

import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object MyRedisUtil {

    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop101").setPort(6379).build()

    def getRedisSink(): RedisSink[(String, String)] = {
        new RedisSink[(String, String)](config, new RedisMapper[(String, String)] {

            override def getCommandDescription: RedisCommandDescription = {
                new RedisCommandDescription(RedisCommand.HSET, "channel_count")
            }

            override def getKeyFromData(data: (String, String)): String = data._1

            override def getValueFromData(data: (String, String)): String = data._2
        })
    }


}
