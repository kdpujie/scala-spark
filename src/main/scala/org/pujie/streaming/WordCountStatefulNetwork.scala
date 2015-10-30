package org.pujie.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * [计算累加值]
 * Spark Streaming的updateStateByKey可以DStream中的数据进行按key做reduce操作，然后对各个批次的数据进行累加。
 * Created by pujie on 2015/10/30.
 */
object WordCountStatefulNetwork {

    def main (args: Array[String]){
        if(args.length != 3){
          println("Usage:  <hostname> <port> <seconds>\n" + "In local mode, <master> should be 'local[n]' with n > 1")
          System.exit(1)
        }
        //状态更新函数
        val updateFunc = (currValues:Seq[Int],preValuesState:Option[Int])=>{
            //通过Spark内部的reduceByKey按Key规约，然后这里传入某Key当前批次的Seq/List，再计算当前批次的总和。
            val currentCount = currValues.sum
            //已累加的值
            val previousCount = preValuesState.getOrElse(0)
            //返回累加后的结果，是一个Option[Int]类型
            Some(currentCount + previousCount)
        }

        val conf = new SparkConf().setAppName("WordCount-Stateful-Network")
        val ssc = new StreamingContext(conf,Seconds(args(2).toInt))
        //创建NetworkInputDStream，需要指定IP和端口
        val lines = ssc.socketTextStream(args(0),args(1).toInt)
        val words = lines.flatMap(_.split(" "))
        val wordDStream = words.map(x=>(x,1))
        //使用updateStateBykey来更新状态
        val stateDStream = wordDStream.updateStateByKey(updateFunc)
        stateDStream.print()
        ssc.start()
        ssc.awaitTermination()
    }
}
