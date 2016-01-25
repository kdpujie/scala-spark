package org.pujie.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 从socket中读取数据，计算单词数量。
 * @author pujie
 */
object WordCountNetwork {
  def main(args:Array[String]){
    // 检查输入参数是否合规
    if(args.length != 3){
      println("Usage:  <hostname> <port> <batch-size>\n" + "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }
    val hostname = args(0)
    val port = args(1).toInt
    val batchSize = args(2).toInt
    val conf = new SparkConf().setAppName("wordCount-Network")
    val ssc = new StreamingContext(conf,Seconds(batchSize))
    val lines = ssc.socketTextStream(hostname, port, StorageLevel.MEMORY_ONLY_SER)
    val words = lines.flatMap { x => x.split(" ") }
    val wordsCounts = words.map (x => (x,1)).reduceByKey( (x,y)=>x+y )
    wordsCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}