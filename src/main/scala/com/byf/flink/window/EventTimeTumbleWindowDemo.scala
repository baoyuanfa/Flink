package com.byf.flink.window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable

object EventTimeTumbleWindowDemo {
    def main(args: Array[String]): Unit = {
        val tool: ParameterTool = ParameterTool.fromArgs(args)
        val host: String = tool.get("host")
        val port: Int = tool.get("port").toInt

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)


        val socketDStream: DataStream[String] = env.socketTextStream(host, port)
        import org.apache.flink.api.scala.createTypeInformation
        val wordTimeCount: DataStream[(String, Long, Long)] = socketDStream.map(data => {
            val fields: Array[String] = data.split(" ")
            (fields(0), fields(1).toLong, 1L)
        })

        wordTimeCount.print("textkey:::")
        val eventTimeDStream: DataStream[(String, Long, Long)] = wordTimeCount.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Long)](Time.milliseconds(1000)) {
                override def extractTimestamp(element: (String, Long, Long)): Long = {
                    element._2
                }
            })
        val keyedEventTimeStream: KeyedStream[(String, Long, Long), Tuple] = eventTimeDStream.keyBy(0)


        val eventWindowStream: WindowedStream[(String, Long, Long), Tuple, TimeWindow] = keyedEventTimeStream.window(TumblingEventTimeWindows.of(Time.seconds(2)))

        val resDStream: DataStream[(String, Long, Long)] = eventWindowStream.sum(2)

        /*val resDStream: DataStream[mutable.HashSet[Long]] = eventWindowStream.fold(new mutable.HashSet[Long]()) { case (set, (key, ts, count)) =>
            set += ts
        }*/

        resDStream.print("window:::")

        env.execute()
    }
}
