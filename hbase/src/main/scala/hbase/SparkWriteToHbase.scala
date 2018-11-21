package hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * hbaseOutput optimization
  *
  * 执行需要传入一个orc文件路径,作为要导入hbase的数据
  */
class SparkWriteToHbase {

}

object SparkWriteToHbase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkHbasePutPartition").setMaster("local[*]")
    conf.set("hbase.client.write.buffer", "16m") // 16MB Buffer

    val sc = new SparkContext(conf)

//  配置hbaseconf,设置固定的输出格式,key是不可变的字符串,value是Put对象
//  设置key 和 value的输出类型
    val hbaseConf: Configuration = HBaseConfiguration.create
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "dcx_test")
    hbaseConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[ImmutableBytesWritable]].getName)
    hbaseConf.set("mapreduce.job.output.key.class", classOf[ImmutableBytesWritable].getName)
    hbaseConf.set("mapreduce.job.output.value.class", classOf[Put].getName)


    val orcPath = args(0)
//  创建df,把orc数据放到spark-sql的临时表里
    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()


    val df: DataFrame = session.read.orc(orcPath)
//  创建临时视图,里边有orc数据,
    df.createOrReplaceTempView("user_install_statusTemp")

//  根据业务写sql,去执行
    val dfRes: DataFrame = session.sql("select pkgname,num from (select pkgname,count(1) as num from user_install_statusTemp group by pkgname) a where a.num>3000 limit 200")

    val rdd: RDD[Row] = dfRes.rdd

    val hbaseRDD: RDD[(ImmutableBytesWritable, Put)] = rdd.mapPartitions(row => {
      val list: List[Row] = row.toList
      println(list)
//    accumulator累加器,计数. 看遍历多少次
      import scala.collection.mutable.ListBuffer
      val resultList = new ListBuffer[(ImmutableBytesWritable, Put)]
      // 在外面new出来在循环里面重复使用,避免了多次new对象造成内存浪费
      val writable = new ImmutableBytesWritable()
      for (next <- list) {
        val put = new Put(Bytes.toBytes("spark_part_" + next.getString(0)))
        put.addColumn(Bytes.toBytes("familyC"), Bytes.toBytes("count"), Bytes.toBytes(next.getLong(1)))
        writable.set(put.getRow)
        resultList += ((writable, put))
      }
      resultList.toIterator
    })

//  coalesce底层默认shuffle=false,从多分区变为少分区时不会产生shuffle
//  控制文件输出数量
    val coalesceReal:RDD[(ImmutableBytesWritable,Put)] = hbaseRDD.coalesce(3)

    coalesceReal.saveAsNewAPIHadoopDataset(hbaseConf)
  }
}
