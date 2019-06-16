package com.byf.flink.app

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object StreamWCApp {
    def main(args: Array[String]): Unit = {

        val tool: ParameterTool = ParameterTool.fromArgs(args)
        val host: String = tool.get("host")
        val port: Int = tool.get("port").toInt

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val socketDStream: DataStream[String] = env.socketTextStream(host, port)
        import org.apache.flink.api.scala.createTypeInformation
        val wordToCountDStream: DataStream[(String, Int)] = socketDStream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)
//        val keyedStream: KeyedStream[(String, Int), Tuple] = socketDStream.flatMap(_.split(" ")).map((_,1)).keyBy(0)

        wordToCountDStream.print().setParallelism(1).setParallelism(1)
//        keyedStream.print().setParallelism(1)
        env.execute()

    }
}
