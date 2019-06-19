package com.ttff.flink.test

import java.time.ZoneId

import org.apache.flink.api.common.serialization.{BulkWriter, SimpleStringEncoder}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.filesystem.{PartFileInfo, RollingPolicy, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
import org.apache.hadoop.io.{IntWritable, Text}

object DefineChenkPoint {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置checkpoint持久化
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
      checkpointConfig

    env.setStateBackend(new RocksDBStateBackend("hdfs:///checkpoints-data/"))
    //设置stateBackend持久化位置
//    env.setStateBackend(new FsStateBackend("hdfs:///checkpoints-data/"))

    //Streaming file sink
      /*val hdfsSink: StreamingFileSink[String] = StreamingFileSink
        .forBulkFormat(new Path("hdfs:///flinkStreaming2hdfs"), BulkWriter.Factory[String])
        .withBucketAssigner(new DateTimeBucketAssigner[String]("yyyy-MM-dd--HHmm"))
        .withBucketCheckInterval(1000 * 60 * 60)
        .build()*/

      //Streaming file sink
      /*val hdfsSink: StreamingFileSink[String] = StreamingFileSink
        .forRowFormat(new Path("hdfs:///flinkStreaming2hdfs"), new SimpleStringEncoder <> ("UTF-8"))
        .withRollingPolicy(new RollingPolicy[String, String] {
            override def shouldRollOnCheckpoint(partFileState: PartFileInfo[String]): Boolean = {
                partFileState.getSize > 128 * 1024 * 1024
            }

            override def shouldRollOnEvent(partFileState: PartFileInfo[String], element: String): Boolean = false

            override def shouldRollOnProcessingTime(partFileState: PartFileInfo[String], currentTime: Long): Boolean = {
                (partFileState.getCreationTime - partFileState.getLastUpdateTime) > 1000 * 60 * 60
            }
        })
        .build()*/

//    hdfsSink.withBucketCheckInterval()

    //Rolling file sink(java)
    /*val sink = new BucketingSink[Tuple2[IntWritable, Text]]("/base/path")
    sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm", ZoneId.of("America/Los_Angeles")))
    sink.setWriter(new SequenceFileWriter[IntWritable, Text])
    sink.setBatchSize(1024 * 1024 * 128) // this is 128 MB,
    sink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins*/
  }
}
