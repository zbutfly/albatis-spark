package net.butfly.albatis.elastic;

import com.sun.istack.internal.NotNull;
import net.butfly.albacore.io.URISpec;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import java.util.Map;


/**
 * Created by 党楚翔 on 2018/11/29.
 */
public class ESInputTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("esInput");

        @NotNull
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();

        URISpec uri = new URISpec("es://hzcominfo@172.30.10.31:39200/test_phga_search/M2ES_CZRK");

//        @NotNull
//        URISpec uri = new URISpec("es://hzcominfo@172.30.10.31:39200/xsga_dpc_test");

        @NotNull
        SparkESInput esInput = new SparkESInput(session,uri);

        Map<String, String> options = esInput.options();
        for (Map.Entry map : options.entrySet()){
            System.out.println(map.getKey()+"\t"+"value="+map.getValue());
        }
    }
}
