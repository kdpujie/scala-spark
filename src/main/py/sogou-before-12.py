#-*- coding:utf-8 -*-
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
conf = SparkConf()
conf.setAppName('sogou-before-12')
conf.setMaster('yarn-client')
spark = SparkContext(conf=conf)
lines = spark.textFile('hdfs://172.17.110.2:8020/tmp/sougou/SogouQ.reduced')
filter_lines = lines.filter(lambda line:line.split('\t')[0] < '00:12:00')
print 'user counts before 12 :' + str(filter_lines.count())
