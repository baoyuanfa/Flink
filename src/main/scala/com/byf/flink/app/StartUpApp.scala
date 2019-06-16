package com.byf.flink.app

import com.alibaba.fastjson.JSON
import com.byf.flink.bean.StartUpLog
import com.byf.flink.util.{MyESUtil, MyJdbcSink, MyKafkaUtil, MyRedisUtil}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object StartUpApp {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val kafkaSource: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer("GMALL_STARTUP")
        val kafkaProducer: FlinkKafkaProducer011[String] = MyKafkaUtil.getProducer("GMALL_UNION")

        import org.apache.flink.api.scala.createTypeInformation

        val kaDStream: DataStream[String] = env.addSource(kafkaSource)

        /*val keyedStream: KeyedStream[(String, Int), Tuple] = kaDStream.map(jsonString => JSON.parseObject(jsonString, classOf[StartUpLog])).map(startUpLog => (startUpLog.ch,1)).keyBy(0)

        //将相同渠道的相加
        val resultDStream: DataStream[(String, Int)] = keyedStream.reduce((startUpLog1, startUpLog2) => {
            (startUpLog1._1, startUpLog1._2 + startUpLog2._2)
        })*/

//        resultDStream.print()

        //将appstore渠道和其他渠道分隔
        /*val staratUpLogDStream: DataStream[StartUpLog] = kaDStream.map(jsonString => JSON.parseObject(jsonString, classOf[StartUpLog]))
        val splitedStream: SplitStream[StartUpLog] = staratUpLogDStream.split(staratUpLog => {
            var list: List[String] = null
            if ("appstore".equals(staratUpLog.ch)) {
                list = List("appstore")
            } else {
                list = List("other")
            }
            list
        })
        val appDS: DataStream[StartUpLog] = splitedStream.select("appstore")
        val otherDS: DataStream[StartUpLog] = splitedStream.select("other")*/

//        appDS.print("appstore: ").setParallelism(1)
//        otherDS.print("other : ").setParallelism(1)

        //合并两个流:connect,map
        /*val connectedStream: ConnectedStreams[StartUpLog, StartUpLog] = appDS.connect(otherDS)
        val coMapDS: DataStream[String] = connectedStream.map(
            (lg1: StartUpLog) => lg1.ch,
            (lg2: StartUpLog) => lg2.ch
        )
        coMapDS.print("connect : ").setParallelism(1)*/

        //union合并两个流(两个流类型必须一样)
        /*val allDS: DataStream[StartUpLog] = appDS.union(otherDS)
        allDS.print("union:").setParallelism(1)

        //kafkaSink
        allDS.map(_.toString).addSink(kafkaProducer)*/

        //redisSink
        /*val countToString: DataStream[(String, String)] = resultDStream.map(data => (data._1, data._2 + ""))
        countToString.addSink(MyRedisUtil.getRedisSink())*/

        //esSink
        /*val esSink: ElasticsearchSink[String] = MyESUtil.getEsSinkFunction("flink_es")
        kaDStream.addSink(esSink)*/

        //jdbcSink
        val startUpLogDS: DataStream[StartUpLog] = kaDStream.map(jsonString => JSON.parseObject(jsonString, classOf[StartUpLog]))
        val arrDStream: DataStream[Array[Any]] = startUpLogDS.map(startUpLog => Array(startUpLog.mid, startUpLog.uid, startUpLog.ch, startUpLog.ts))

        val sql: String ="insert into flink_mysql values(?, ?, ? ,?)"
        val jdbcSink = new MyJdbcSink(sql)

        arrDStream.addSink(jdbcSink)

        env.execute()
    }
}
