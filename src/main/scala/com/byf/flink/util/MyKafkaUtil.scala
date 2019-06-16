package com.byf.flink.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object MyKafkaUtil {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "byf")

    def getConsumer(topic: String): FlinkKafkaConsumer011[String] = {
        val myKafkaConsumer = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties)
        myKafkaConsumer
    }

    def getProducer(topic: String): FlinkKafkaProducer011[String] = {
        val myKafkaProducer = new FlinkKafkaProducer011[String](
            "hadoop102:9092,hadoop103:9092,hadoop104:9092",
            topic,
            new SimpleStringSchema())
        myKafkaProducer
    }
}
