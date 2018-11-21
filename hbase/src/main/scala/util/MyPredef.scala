package util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by 党楚翔 on 2018/7/6.
  *
  * 自动删除hdfs输出目录,防止下次执行报文件已存在的错误
  */
object MyPredef {
  implicit def deleteHdfs(outpath1:String) = new autoDeleteOutputPath(outpath1:String)
}

class autoDeleteOutputPath(outpath:String){
  def deletePath()={
    val conf = new Configuration()
    val fs: FileSystem = FileSystem.get(conf)
    val path = new Path(outpath)
    if (fs.exists(path)){
      //    true表示recursive删除
      fs.delete(path,true)
    }
  }
}
