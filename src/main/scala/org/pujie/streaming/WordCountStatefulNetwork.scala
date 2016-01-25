package org.pujie.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * [带状态的操作][计算累加值]
 * Spark Streaming的updateStateByKey可以DStream中的数据进行按key做reduce操作，然后对各个批次的数据进行累加。
 * UpdateStateByKey原语用于记录历史记录。若不用UpdateStateByKey来更新状态，那么每次数据进来后分析完成后，结果输出后将不在保存。
 * 使用UpdateStateByKey原语需要用于记录的State，可以为任意类型，如上例中即为Optional<Intege>类型；此外还需要更新State的函数。
 * 1，
 * Created by pujie on 2015/10/30.
 */
object WordCountStatefulNetwork {

    def main (args: Array[String]){
        if(args.length != 4){
          println("Usage:  <hostname> <port> <seconds> <checkpoint>\n" + "In local mode, <master> should be 'local[n]' with n > 1")
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
        //使用updateStateByKey前需要设置checkpoint
        ssc.checkpoint(args(3))
        //创建NetworkInputDStream，需要指定IP和端口
        val lines = ssc.socketTextStream(args(0),args(1).toInt)
        val words = lines.flatMap(_.split(" "))
        val pairs = words.map(x=>(x,1))
        //使用updateStateBykey来更新状态
        val stateDStream = pairs.updateStateByKey(updateFunc)
        stateDStream.print()
        ssc.start()
        ssc.awaitTermination()
    }
}
