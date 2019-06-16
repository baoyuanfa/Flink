package com.byf.flink.hdfs

import com.byf.flink.util.MyKafkaUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


object Kafka2HDFS {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val kafkaSource: FlinkKafkaConsumer011[String] = MyKafkaUtil.getConsumer("test")

        import org.apache.flink.api.scala._
        val kafkaDStream: DataStream[String] = env.addSource(kafkaSource)

        val hdfsSink = new BucketingSink[String]("hdfs://hadoop102:9000/flink2hdfs")
        hdfsSink.setWriter(new StringWriter()).setBatchSize(20).setBatchRolloverInterval(2000)
        kafkaDStream.addSink(hdfsSink)

        env.execute()

        /*new BucketingSink[Tuple2[IntWritable, Text]]("/flinkk2hdfs")


        val sink = new BucketingSink[Tuple2[IntWritable, Text]]("/base/path")
        sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm", ZoneId.of("America/Los_Angeles")))
        sink.setWriter(new SequenceFileWriter[IntWritable, Text])
        sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
        sink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins*/

    }
}
