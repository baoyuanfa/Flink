package com.byf.flink.util

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests


object MyESUtil {

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102",9200,"http"))
    httpHosts.add(new HttpHost("hadoop103",9200,"http"))
    httpHosts.add(new HttpHost("hadoop104",9200,"http"))

    def getEsSinkFunction(indexName : String): ElasticsearchSink[String] = {
        val esFunction: ElasticsearchSinkFunction[String] = new ElasticsearchSinkFunction[String] {
            override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
                println("尝试写入:" + element)
                val jsonObject: JSONObject = JSON.parseObject(element)
                val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(jsonObject)
                indexer.add(indexRequest)
                println("成功写入一条数据")
            }
        }


        val esSinkBuilder = new ElasticsearchSink.Builder[String](httpHosts,esFunction)
        esSinkBuilder.setBulkFlushMaxActions(10)

        esSinkBuilder.build()
    }
}
