package com.byf.flink.window

import com.byf.flink.util.MyKafkaUtil
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.windows._

object EventTimeSlidWindowDemo {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val socketDStream: DataStream[String] = env.socketTextStream("hadoop102",7777)
        import org.apache.flink.api.scala.createTypeInformation
        val wordTimeCount: DataStream[(String, Long, Long)] = socketDStream.map(data => {
            val fields: Array[String] = data.split(" ")
            (fields(0), fields(1).toLong, 1L)
        })
        wordTimeCount.print("text ::: ")
        val textWaterMarkedDStream: DataStream[(String, Long, Long)] = wordTimeCount.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Long)](Time.milliseconds(1000)) {
                override def extractTimestamp(element: (String, Long, Long)): Long = {
                    element._2
                }
            }
        )

        val keyedStream: KeyedStream[(String, Long, Long), Tuple] = textWaterMarkedDStream.keyBy(0)

        val slidwindowStream: WindowedStream[(String, Long, Long), Tuple, TimeWindow] = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(4),Time.seconds(2)))
        val resDStream: DataStream[(String, Long, Long)] = slidwindowStream.sum(2)

        resDStream.print("window:::")
        env.execute()
    }
}