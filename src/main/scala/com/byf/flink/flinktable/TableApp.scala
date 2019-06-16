package com.byf.flink.flinktable

import com.alibaba.fastjson.JSON
import com.byf.flink.bean.StartUpLog
import com.byf.flink.util.MyKafkaUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._

object TableApp {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val kafkaConsumer: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer("GMALL_STARTUP")
        val kafkaDStream: DataStream[String] = env.addSource(kafkaConsumer)

        val startUpLogDStream: DataStream[StartUpLog] = kafkaDStream.map( jsonString => JSON.parseObject(jsonString, classOf[StartUpLog]))
        val waterMarkDStream: DataStream[StartUpLog] = startUpLogDStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StartUpLog](Time.milliseconds(0L)) {
            override def extractTimestamp(element: StartUpLog): Long = {
                element.ts
            }
        }).setParallelism(1)



        val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

        val startUpLogTable: Table = tableEnv.fromDataStream(waterMarkDStream, 'mid,'uid,'appid,'area,'os,'ch,'logType,'vs,'logDate,'logHour,'logHourMinute,'ts.rowtime)

        /*val appstoreTable: Table = startUpLogTable.select("mid, uid, ch, ts").filter("ch = 'appstore'")

        val resDStream: DataStream[(String, String, String, Long)] = appstoreTable.toAppendStream[(String, String, String, Long)]*/

        //每10秒统计一次各个渠道的个数,
        /*val groupByCh: Table = startUpLogTable.window(Tumble over 10000.millis on 'ts as 'tt).groupBy("ch,tt").select('ch, 'ch.count)

        val resDStream: DataStream[(Boolean, (String, Long))] = groupByCh.toRetractStream[(String, Long)]*/

        // 通过sql 进行操作

        //val resultSQLTable : Table = tableEnv.sqlQuery( "select ch ,count(ch)   from "+startupTable+"  group by ch   ,Tumble(ts,interval '10' SECOND )")
        val resTable: Table = tableEnv.sqlQuery("select ch, count(ch) from " + startUpLogTable + " group by ch, Tumble(ts,interval '10' SECOND)")
        val resDStream: DataStream[(Boolean, (String, Long))] = resTable.toRetractStream[(String, Long)]
        resDStream.filter(_._1).print()
        env.execute()
    }
}
