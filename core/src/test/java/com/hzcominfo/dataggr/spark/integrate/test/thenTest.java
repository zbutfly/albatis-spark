package com.hzcominfo.dataggr.spark.integrate.test;


/**
 * Created by 党楚翔 on 2018/12/4.
 */

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.Connection;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.SparkPump;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

public class thenTest {
    static final Logger logger = Logger.getLogger(thenTest.class);
    private URISpec inputUri = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb/");
    private URISpec outputUri = new URISpec("mongodb://devdb:Devdb1234@172.30.10.31:40012/devdb/");

    public static void main(String[] args) {
        thenTest sparkDemo = new thenTest();
        try {
            sparkDemo.createInput();
//            sparkDemo.exec();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void exec() throws IOException {
        try (
                Connection conn = Connection.connect(outputUri);
                Output<Rmap> out = conn.output(TableDesc.dummy("SPARK_COLLISION_RESULT")); //init spark
                SparkInput<Row> input = createInput().with("taskId", "sparkIO");
                SparkPump pump = input.pump(out)
        )
        {
            pump.open();
        }
    }

    public SparkInput<Row> createInput() throws IOException {
        Connection connect = Connection.connect(inputUri);
        SparkInput<Row> input = (SparkInput) connect.input(TableDesc.dummy("mainTest"));
        List<Tuple2<String, Dataset<Row>>> rows1 = input.rows();

        input.then(item -> item+"_dcx");
        List<Tuple2<String, Dataset<Row>>> rows = input.rows();


        return input;
    }
}