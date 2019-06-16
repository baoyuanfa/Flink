package com.byf.flink.window

import com.alibaba.fastjson.JSON
import com.byf.flink.bean.StartUpLog
import com.byf.flink.util.MyKafkaUtil
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object CountWindowDemo {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val kafkaSource: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer("GMALL_STARTUP")

        import org.apache.flink.api.scala.createTypeInformation

        val kaDStream: DataStream[String] = env.addSource(kafkaSource)

        val keyedStream: KeyedStream[(String, Int), Tuple] = kaDStream.map(jsonString => JSON.parseObject(jsonString, classOf[StartUpLog])).map(startUpLog => (startUpLog.ch,1)).keyBy(0)

        //CountWindow
        //滚动窗口
        //val wdStream: WindowedStream[(String, Int), Tuple, GlobalWindow] = keyedStream.countWindow(10L)

        //滑动窗口
        val wdStream: WindowedStream[(String, Int), Tuple, GlobalWindow] = keyedStream.countWindow(10L,2L)
        val resDStream: DataStream[(String, Int)] = wdStream.sum(1)
        resDStream.print()
    }
}
