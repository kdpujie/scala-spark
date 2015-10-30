package org.pujie.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 从socket中读取数据，计算单词数量。
 * Created by pujie on 2015/10/30.
 */
object WordCountHdfs {

    def main(args:Array[String]): Unit ={
      if(args.length !=2){
        println("Usage:  <directory> <seconds>\n" + "In local mode, <master> should be 'local[n]' with n > 1")
        System.exit(1)
      }
      val sourceDirectory = args(0) //hdfs路径
      val seconds = args(1).toInt  //处理时间间隔。
      val conf = new SparkConf().setAppName("WordCount-hdfs")
      val ssc = new StreamingContext(conf,Seconds(seconds))
      val lines = ssc.textFileStream(sourceDirectory)
      val words = lines.flatMap(x => x.split(" "))
      val wordsCounts = words.map(x=>(x,1)).reduceByKey(_+_) //reduceByKey相当于先执行groupByKey，再对结果做reduce计算。
      wordsCounts.print()
      ssc.start()   //启动任务
      ssc.awaitTermination() //等待被执行
    }
}
