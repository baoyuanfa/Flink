package com.byf.flink.window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object EventTimeSessionWindowDemo {
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

        val textWateredDStream: DataStream[(String, Long, Long)] = wordTimeCount.assignTimestampsAndWatermarks(
            new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Long)](Time.seconds(1)) {
                override def extractTimestamp(element: (String, Long, Long)): Long = {
                    element._2
                }
            }
        )


        val keyedStream: KeyedStream[(String, Long, Long), Tuple] = textWateredDStream.keyBy(0)

        val sessionWindowStream: WindowedStream[(String, Long, Long), Tuple, TimeWindow] = keyedStream.window(EventTimeSessionWindows.withGap(Time.milliseconds(4000)))

        val resDStream: DataStream[(String, Long, Long)] = sessionWindowStream.sum(2)

        resDStream.print("window:::")
        env.execute()
    }
}
