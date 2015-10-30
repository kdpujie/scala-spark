package org.pujie.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * ��socket�ж�ȡ���ݣ����㵥��������
 * Created by pujie on 2015/10/30.
 */
object WordCountHdfs {

    def main(args:Array[String]): Unit ={
      if(args.length !=2){
        println("Usage:  <directory> <seconds>\n" + "In local mode, <master> should be 'local[n]' with n > 1")
        System.exit(1)
      }
      val sourceDirectory = args(0) //hdfs·��
      val seconds = args(1).toInt  //����ʱ������
      val conf = new SparkConf().setAppName("WordCount-hdfs")
      val ssc = new StreamingContext(conf,Seconds(seconds))
      val lines = ssc.textFileStream(sourceDirectory)
      val words = lines.flatMap(x => x.split(" "))
      val wordsCounts = words.map(x=>(x,1)).reduceByKey(_+_) //reduceByKey�൱����ִ��groupByKey���ٶԽ����reduce���㡣
      wordsCounts.print()
      ssc.start()   //��������
      ssc.awaitTermination() //�ȴ���ִ��
    }
}
