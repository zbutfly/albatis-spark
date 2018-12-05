//package com.hzcominfo.dataggr.spark.integrate;
//
//import com.hzcominfo.dataggr.spark.integrate.mongo.SparkMongoInput;
//import com.sun.istack.internal.NotNull;
//import net.butfly.albacore.io.URISpec;
//import net.butfly.albacore.paral.Sdream;
//import net.butfly.albacore.utils.logger.Statistic;
//import net.butfly.albatis.ddl.TableDesc;
//import net.butfly.albatis.io.Input;
//import net.butfly.albatis.io.Rmap;
//import net.butfly.albatis.spark.SparkInput;
//import net.butfly.albatis.spark.SparkMapInput;
//import net.butfly.albatis.spark.impl.SparkConnection;
//import net.butfly.albatis.spark.impl.SparkIO;
//import net.butfly.albatis.spark.input.SparkInnerJoinInput;
//import net.butfly.albatis.spark.input.SparkJoinInput;
//import org.apache.spark.SparkConf;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.SparkSession;
//import scala.Tuple2;
//import java.util.List;
//
///**
// * Created by 党楚翔 on 2018/11/29.
// */
//public class sparkConnectionTest {
//    public static void main(String[] args) {
//        SparkConf conf = new SparkConf();
//        conf.setMaster("local[*]").setAppName("esInput");
//        @NotNull
//        SparkSession session = SparkSession.builder().config(conf).getOrCreate();
//
//        URISpec esUri = new URISpec("es://hzcominfo@172.30.10.31:39200/test_phga_search/M2ES_CZRK");
//
//        URISpec uri1 = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb/SPARK_FEATURE_SUB");
//        URISpec uri2 = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb/SPARK_FEATURE_TEST");
//
//        SparkConnection connection = new SparkConnection(uri1);
//
//        SparkInput input  =  connection.input(uri1);
//        SparkInput input1 =  connection.input(uri2);
//        SparkJoinInput conn = connection.innerJoin(input, "CERTIFIED_ID", input1, "ZJHM");
//
////        SparkJoinInput conn = connection.innerJoin(new SparkMapInput(session, uri1, TableDesc.dummy("PH_ZHK_CZRK")) {
////            @Override
////            protected List<Tuple2<String, Dataset<Rmap>>> load() {
////                return null;
////            }
////        }, "_id", new SparkMapInput(session, uri2, TableDesc.dummy("PH_ZHK_ZTRI")) {
////            @Override
////            protected List<Tuple2<String, Dataset<Rmap>>> load() {
////                return null;
////            }
////        }, "_id");
//
//
//        conn.open();
//
//        String ser = conn.ser();
//
//        String format = conn.format();
//
//        System.out.println(ser +"\t"+format);
//    }
//}
