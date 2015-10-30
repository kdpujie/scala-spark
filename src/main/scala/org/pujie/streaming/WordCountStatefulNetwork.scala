package org.pujie.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * [�����ۼ�ֵ]
 * Spark Streaming��updateStateByKey����DStream�е����ݽ��а�key��reduce������Ȼ��Ը������ε����ݽ����ۼӡ�
 * Created by pujie on 2015/10/30.
 */
object WordCountStatefulNetwork {

    def main (args: Array[String]){
        if(args.length != 3){
          println("Usage:  <hostname> <port> <seconds>\n" + "In local mode, <master> should be 'local[n]' with n > 1")
          System.exit(1)
        }
        //״̬���º���
        val updateFunc = (currValues:Seq[Int],preValuesState:Option[Int])=>{
            //ͨ��Spark�ڲ���reduceByKey��Key��Լ��Ȼ�����ﴫ��ĳKey��ǰ���ε�Seq/List���ټ��㵱ǰ���ε��ܺ͡�
            val currentCount = currValues.sum
            //���ۼӵ�ֵ
            val previousCount = preValuesState.getOrElse(0)
            //�����ۼӺ�Ľ������һ��Option[Int]����
            Some(currentCount + previousCount)
        }

        val conf = new SparkConf().setAppName("WordCount-Stateful-Network")
        val ssc = new StreamingContext(conf,Seconds(args(2).toInt))
        //����NetworkInputDStream����Ҫָ��IP�Ͷ˿�
        val lines = ssc.socketTextStream(args(0),args(1).toInt)
        val words = lines.flatMap(_.split(" "))
        val wordDStream = words.map(x=>(x,1))
        //ʹ��updateStateBykey������״̬
        val stateDStream = wordDStream.updateStateByKey(updateFunc)
        stateDStream.print()
        ssc.start()
        ssc.awaitTermination()
    }
}
