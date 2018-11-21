package hbase
/**
  * Created by 党楚翔
  */
import hbase.{SparkHbaseRead, SparkWriteToHbase}
import org.apache.hadoop.util.ProgramDriver

object Driver {
  def main(args: Array[String]): Unit = {
    val driver = new ProgramDriver
    driver.addClass("sparkhbaseread",classOf[SparkHbaseRead],"spark read")

    driver.addClass("sparkwritetohbase",classOf[SparkWriteToHbase],"spark write")

    driver.run(args)
  }
}

