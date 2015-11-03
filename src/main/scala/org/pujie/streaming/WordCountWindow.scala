package org.pujie.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * [窗口计算]：指定时间间隔内的统计信息。
 * Created by pujie on 2015/11/2.
 */
object WordCountWindow {

  def main (args: Array[String]){
    if(args.length != 7){
      println("Usage:  <hostname> <port> <save-directory> <checkpoint-directory> <batch-size> <window-duration> <slide-duration>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }
    val hostName=args(0)             //socketserver所在地址
    val port = args(1).toInt         //socketserver监听端口
    val saveDirectory = args(2)      //计算结果输出路径
    val checkpointDirectory = args(3)
    val batchSize = args(4).toInt    //spark streaming数据大小，以时间为单位（如10m）
    val windowDuration=args(5).toInt //窗口的大小，以时间为单位
    val slideDuration=args(6).toInt  //滑动窗口的频率，以时间为单位。这两个参数必须是batch size的倍数
    val conf = new SparkConf().setAppName("word-count-window")
    val ssc = new StreamingContext(conf,Seconds(batchSize))
    ssc.checkpoint(checkpointDirectory)
    val lines = ssc.socketTextStream(hostName,port,StorageLevel.MEMORY_ONLY_SER)
    val words = lines.flatMap((x:String)=>x.split(" "))
    val wordPair = words.map((x:String)=>(x,1))
    val wordWindowCount = wordPair.reduceByKeyAndWindow((x:Int,y:Int)=>x+y,_-_,Seconds(windowDuration),Seconds(slideDuration))

//    wordWindowCount.print()
    wordWindowCount.saveAsTextFiles(saveDirectory) //窗口滑动一次，生成一个output文件。
    ssc.start()
    ssc.awaitTermination()

  }
}
