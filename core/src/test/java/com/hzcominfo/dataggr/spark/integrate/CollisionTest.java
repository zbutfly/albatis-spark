//package com.hzcominfo.dataggr.spark.integrate;
//
///**
// * Created by 党楚翔 on 2018/12/4.
// */
//import com.hzcominfo.dataggr.spark.integrate.kafka.SparkKafkaEtlInput;
//import com.hzcominfo.dataggr.spark.integrate.mongo.SparkMongoInput;
//import com.sun.rowset.internal.Row;
//import net.butfly.albacore.io.URISpec;
//import net.butfly.albacore.utils.logger.Logger;
//import net.butfly.albatis.io.pump.Pump;
//import net.butfly.albatis.spark.SparkOutput;
//import net.butfly.albatis.spark.impl.SparkConnection;
//import net.butfly.albatis.spark.input.SparkJoinInput;
//import net.butfly.albatis.spark.util.InputMapTool;
//
//
//import org.apache.spark.sql.Row;
//
//import com.hzcominfo.dataggr.spark.integrate.kafka.SparkKafkaEtlInput;
//import com.hzcominfo.dataggr.spark.integrate.mongo.SparkMongoInput;
//import com.hzcominfo.dataggr.spark.io.SparkConnection;
//import com.hzcominfo.dataggr.spark.io.SparkOutput;
//import com.hzcominfo.dataggr.spark.join.SparkJoinInput;
//import com.hzcominfo.dataggr.spark.util.InputMapTool;
//
//import net.butfly.albacore.io.URISpec;
//import net.butfly.albacore.utils.logger.Logger;
//import net.butfly.albatis.io.pump.Pump;
//
//public class CollisionTest {
//    static final Logger logger = Logger.getLogger(CollisionTest.class);
//
//    public static void main(String[] args) {
//        logger.info("Calcing : [CollisionTest]");
//        URISpec kiu = new URISpec("kafka:etl://data01:9092,data02:9092,data03:9092/ZHW_TLGA_GNSJ_NEW_1_1");
//        URISpec table1 = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb/SPARK_FEATURE_SUB");
//        URISpec table2 = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb/SPARK_FEATURE_TEST");
////		URISpec ou = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb/SPARK_FEATURE_AGG");
//
//        try (SparkConnection sc = new SparkConnection(kiu);
//             SparkMongoInput min = sc.input(table1);
//             SparkKafkaEtlInput kin = sc.input(kiu);
////             SparkJoinInput jin = sc.innerJoin(kin, "CERTIFIED_ID", new InputMapTool().append(min, "ZJHM").get());
//             sc.innerJoin(kin,"CERTIFIED_ID",min,"XJHM");
//             SparkOutput out = sc.output(table2);
//             SparkPump p = jin.pump(out)) {
//             kin.schema(FeatureTest.SAMPLE_SCHEMA);
//            p.open();
//            System.out.println("starting...");
//        } catch (Throwable t) {
//            logger.error("failed", t);
//        } finally {
//        }
//        System.out.println("end...");
//    }
//}