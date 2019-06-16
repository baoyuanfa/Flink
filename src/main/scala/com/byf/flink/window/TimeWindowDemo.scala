package com.byf.flink.window

import com.alibaba.fastjson.JSON
import com.byf.flink.bean.StartUpLog
import com.byf.flink.util.MyKafkaUtil
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object TimeWindowDemo {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val kafkaSource: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer("GMALL_STARTUP")

        import org.apache.flink.api.scala.createTypeInformation

        val kaDStream: DataStream[String] = env.addSource(kafkaSource)

        val keyedStream: KeyedStream[(String, Int), Tuple] = kaDStream.map(jsonString => JSON.parseObject(jsonString, classOf[StartUpLog])).map(startUpLog => (startUpLog.ch,1)).keyBy(0)
        //TimeWindow
        //滚动窗口
        //val wdStream: WindowedStream[(String, Int), Tuple, TimeWindow] = keyedStream.timeWindow(Time.seconds(10))
        //滑动窗口
        val wdStream: WindowedStream[(String, Int), Tuple, TimeWindow] = keyedStream.timeWindow(Time.seconds(10),Time.seconds(5))
        val resDStream: DataStream[(String, Int)] = wdStream.sum(1)
        resDStream.print()
    }
}
