package com.hzcominfo.dataggr.spark.integrate;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.spark.output.SparkSaveOutput;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Created by 党楚翔 on 2018/11/29.
 */
public class sparkSaveOutputTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("hiveSaveOutput");
//        Map<String, String> options = Maps.of();
//        options.put("kafka.bootstrap.servers", "data01:9092,data02:9092,data03:9092");
//        options.put("subscribe", "ZHK_QBZX_LGZS_NEW");
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();

        URISpec uriSpec = new URISpec("zk://data01:2181,data02:2181,data03:2181/kafka");


        SparkSaveOutput output = new SparkSaveOutput(session,uriSpec,TableDesc.dummy("testTable"));

    }
}
