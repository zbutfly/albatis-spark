package com.hzcominfo.dataggr.spark.integrate;

import com.sun.istack.internal.NotNull;
import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.impl.SparkConnection;
import net.butfly.albatis.spark.impl.SparkIO;
import net.butfly.albatis.spark.input.SparkInnerJoinInput;
import net.butfly.albatis.spark.input.SparkJoinInput;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

/**
 * Created by 党楚翔 on 2018/11/29.
 */
public class sparkConnectionTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("esInput");

        @NotNull
        SparkSession session = SparkSession.builder().config(conf).getOrCreate();

        SparkConnection connection = new SparkConnection();

        URISpec esUri = new URISpec("es://hzcominfo@172.30.10.31:39200/test_phga_search/M2ES_CZRK");


        URISpec czrkUri = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb.PH_ZHK_CZRK");


        URISpec ztriURI = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb.PH_ZHK_ZTRI");


        SparkJoinInput sparkJoinInput = connection.orJoin(new SparkInput<Rmap>(session, czrkUri, TableDesc.dummy("PH_ZHK_CZRK")) {
            @Override
            protected <T> List<Tuple2<String, Dataset<T>>> load() {
                return (List<Tuple2<String, Dataset<T>>>) new Tuple2<String, String>(czrkUri.getFile(), czrkUri.getHost());
            }
        }, "col", new SparkInput<Rmap>(session, ztriURI, TableDesc.dummy("PH_ZHK_ZTRI")) {
            @Override
            protected <T> List<Tuple2<String, Dataset<T
                    >>> load() {
                return (List<Tuple2<String, Dataset<T>>>) new Tuple2<Integer, Integer>(ztriURI.getDefaultPort(),1);
            };
        }, "_id");





        sparkJoinInput.open();

        sparkJoinInput.close();





    }
}
