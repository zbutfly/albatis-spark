package net.butfly.albatis.kudu;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import org.apache.kudu.spark.kudu.*;
import org.apache.kudu.client.*;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.spark.SparkRowInput;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import scala.Tuple2;

import static net.butfly.albatis.spark.impl.Sparks.split;


@Schema("kudu")
public class SparkKuduInput extends SparkRowInput implements SparkKudu {
    private static final long serialVersionUID = 5472880102313131224L;
    private static String HTTP_PORT = "httpport";

    public SparkKuduInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
        super(spark, targetUri, null, table);
    }

    @Override
    public Map<String, String> options() {
        Map<String, String> options = kuduOpts(targetUri);
        options.put("kudu.table", table().name);
        return options;
    }

    @Override
    public String format() {
        return "org.apache.kudu.spark.kudu";
    }

    @Override
    protected List<Tuple2<String, Dataset<Row>>> load() {
        List<List<Tuple2<String, Dataset<Row>>>> list = Colls.list(schemaAll().values(), item -> {
            Map<String, String> options = options();
//            Dataset<Row> rowDataset = spark.read().format(format()).options(options).load();

            Dataset<Row> rowDataset =  spark.read().format(format()).option("kudu.master",options.get("kudu.master")).option("kudu.table",options.get("kudu.table")).load();

            return Colls.list(split(rowDataset, false), ds -> new Tuple2<>(item.name, ds.persist(StorageLevel.OFF_HEAP())));
        });
        return Colls.flat(list);
    }
}
