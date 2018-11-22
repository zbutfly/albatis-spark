package hbase

import util.ORCUtil
import util.MyPredef.deleteHdfs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
  * spark读取hbase到hdfs
  *
  * 比如读取
  * create 'dcx_test',{NAME=>'familyC',VERSION=>1,COMPRESSION=>'snappy'}
  */

class SparkHbaseRead {

}

object SparkHbaseRead{
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("要有两个参数,输入目录 输出目录")
      return
    }
    //输出路径
    val hdfsOutPath = args(1)
    if (!hdfsOutPath.isEmpty){
      hdfsOutPath.deletePath()
    }
    //  第一个args是输入路径
    val orcPath = args(0)
    val conf = new SparkConf()
    conf.setAppName("SparkHbaseBulkLoad").setMaster("local[*]")
    conf.set("spark.serialize",classOf[KryoSerializer].getName)
    conf.registerKryoClasses(Array[Class[_]](classOf[ORCUtil]))

    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val dataFrame: DataFrame = session.read.orc(orcPath)

//  先拿10000条数据
    val rdd: RDD[Row] = dataFrame.limit(10000).rdd

    //  生成元组,放到Hbase里  df里取10000条数据,转成rdd,用mapp
    val hbaseData: RDD[(ImmutableBytesWritable, KeyValue)] = rdd.mapPartitions(row => {
      val rowList: List[Row] = row.toList
      import scala.collection.mutable.ListBuffer
      val resultlist = new ListBuffer[(ImmutableBytesWritable, KeyValue)]

      for(row <- rowList){
        val rk = new ImmutableBytesWritable()
        rk.set(Bytes.toBytes("rowkeyPrefix" + row.getString(1)))
        val keyValue = new KeyValue(rk.get(),Bytes.toBytes("familyC"),Bytes.toBytes("columnName"),Bytes.toBytes(row.getString(4)))
        resultlist += ((rk,keyValue))
      }
      resultlist.toIterator
    }).sortByKey()


    val hbaseConf: Configuration = HBaseConfiguration.create()
//  指定要导出的表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE,"dcx_test")

    //  设置任务的rowkey 列输出类型,
    val job: Job = Job.getInstance(hbaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])

    val connection: Connection = ConnectionFactory.createConnection(hbaseConf)
    val tableName: TableName = TableName.valueOf("dcx_test")
    //  拿到htable对象
    val table: HTable = connection.getTable(tableName).asInstanceOf[HTable]

    val descriptor: HTableDescriptor = table.getTableDescriptor

    val locator: RegionLocator = table.getRegionLocator


    //   导出hfile文件
    HFileOutputFormat2.configureIncrementalLoad(job,table.getTableDescriptor,table.getRegionLocator)


    //  这一步就导出hfile数据到hdfs了
    hbaseData.saveAsNewAPIHadoopFile(hdfsOutPath,classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],job.getConfiguration)

  }

}
