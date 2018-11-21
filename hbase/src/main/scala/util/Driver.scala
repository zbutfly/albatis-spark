package util

import hbase.{SparkHbaseRead, SparkWriteToHbase}
import org.apache.hadoop.util.ProgramDriver

class Driver{

}

object Driver {
  def main(args: Array[String]): Unit = {
    val driver = new ProgramDriver

    driver.addClass("SparkHbaseRead",classOf[SparkHbaseRead],"spark read hbase write to hdfs")
    driver.addClass("SparkWriteToHbase",classOf[SparkWriteToHbase],"spark read hdfs write to hbase")

    driver.run(args)
  }
}
