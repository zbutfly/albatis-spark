package net.butfly.albatis.kudo;

import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.collection.Colls;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import org.apache.kudu.spark.kudu.*;
import org.apache.kudu.client.*;


import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.spark.SparkRowInput;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import scala.Tuple2;

import static net.butfly.albatis.spark.impl.Sparks.split;

@Schema("kudo")
public class SparkKudoInput extends SparkRowInput {
	private static final long serialVersionUID = 5472880102313131224L;
	private static String HTTP_PORT = "httpport";

	public SparkKudoInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri, TableDesc.dummy(targetUri.getPath()));
	}

	@Override
	public Map<String, String> options() {
		Map<String, String> options = Maps.of();
		options.put("kudu.master", "kudu://172.30.10.31:7051,172.30.10.32:7051,172.30.10.33:7051");
		options.put("kudu.table", table().name);
		options.put("path", targetUri.getPath());
		return options;
	}

	@Override
	public String format() {
		return "kudo";
	}

	@Override
	protected List<Tuple2<String, Dataset<Row>>> load() {
		List<List<Tuple2<String, Dataset<Row>>>> list = Colls.list(schemaAll().values(), item -> {
			Map<String, String> options = options();

			KuduContext kuduContext = new KuduContext(options.get("kudu.master"), spark.sparkContext());

			DataFrameReader opt = spark.read().options(options);



			Dataset<Row> rdd = opt.load(options.get("path"));

			return Colls.list(split(rdd, false), ds -> new Tuple2<>(item.name, ds.persist(StorageLevel.OFF_HEAP())));
		});
		return Colls.flat(list);
	}
}
