package com.hzcominfo.dataggr.spark.integrate;

import com.sun.istack.internal.NotNull;
import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.impl.SparkConnection;
import net.butfly.albatis.spark.input.JoinType;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Created by 党楚翔 on 2018/12/5.
 */
public class sparkConnectionNewTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("esInput");
        @NotNull
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();

        URISpec uri1 = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb/SPARK_FEATURE_SUB");
        URISpec uri2 = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb/SPARK_FEATURE_TEST");

        SparkConnection connection = new SparkConnection(uri1);
        SparkInput<Object> sub = connection.input(uri1, TableDesc.dummy("sub"));
        SparkInput<Object> test = connection.input(uri2, TableDesc.dummy("test"));

        connection.innerJoinNew(uri1,uri2,"CERTIFIED_ID","ZJHM",JoinType.inner);

    }
}
