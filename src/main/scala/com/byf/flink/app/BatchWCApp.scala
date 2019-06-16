package com.byf.flink.app

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

object BatchWCApp {
    def main(args: Array[String]): Unit = {

        val tool: ParameterTool = ParameterTool.fromArgs(args)
        val input: String = tool.get("input")
        val output: String = tool.get("output")

        val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        val fileDS: DataSet[String] = environment.readTextFile(input)
        import org.apache.flink.api.scala.createTypeInformation
        val aggWordToCount: AggregateDataSet[(String, Int)] = fileDS.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

//        aggWordToCount.setParallelism(1).print()
        aggWordToCount.setParallelism(1).writeAsCsv(output)
        environment.execute()
//        environment.execute()
    }
}
