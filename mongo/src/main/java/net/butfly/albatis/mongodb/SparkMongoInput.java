package net.butfly.albatis.mongodb;

import static net.butfly.albatis.spark.impl.Sparks.split;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.Albatis;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.spark.SparkRowInput;
import net.butfly.albatis.spark.impl.SparkConf;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

@Schema("mongodb")
@SparkConf(key = "spark.mongodb.input.uri", value = "mongodb://127.0.0.1/FxxkMongoSpark.FakeCollection")
public class SparkMongoInput extends SparkRowInput implements SparkMongo {
	private static final long serialVersionUID = 2110132305482403155L;

	public SparkMongoInput(SparkSession spark, URISpec targetUri, TableDesc... table) throws IOException {
		super(spark, targetUri, null, table);
	}

	@Override
	public Map<String, String> options() {
		Map<String, String> opts = mongoOpts(targetUri);
		opts.put("partitioner", "MongoSamplePartitioner");
		return opts;
	}

	@Override
	protected List<Tuple2<String, Dataset<Row>>> load() {
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		List<List<Tuple2<String, Dataset<Row>>>> resultList = Colls.list(schemaAll().values(), item -> {
			Map<String, String> opts = options();
			opts.put("collection", item.name);
			ReadConfig rc = ReadConfig.create(opts);

			FieldDesc[] fields = item.fields();

			List<String> fieldList = new ArrayList<>();
			for (FieldDesc desc : fields) {
				String name = desc.name;
				fieldList.add(name);
			}

			JavaMongoRDD<Document> rdd = MongoSpark.load(jsc, rc);
			if (Systems.isDebug()) {
				int limit = Integer.parseInt(Configs.gets(Albatis.PROP_DEBUG_INPUT_LIMIT, "-1")) / rdd.getNumPartitions() + 1;
				if (limit > 0) rdd = rdd.withPipeline(Colls.list(Document.parse("{ $limit: " + limit + " }")));
			}
			List<Column> columnList = new ArrayList<>();

			Dataset<Row> ds = rdd.toDF();

			fieldList.forEach(f -> columnList.add(ds.col(f)));

			Dataset<Row> resultDS = ds.select(convertListToSeq(columnList));
			// d = d.withColumn(FIELD_TABLE_NAME, lit(t.name)).withColumn(FIELD_KEY_VALUE, d.col("_id.oid")).withColumn("_id", d.col(
			// "_id.oid"));
			return Colls.list(split(resultDS, false), ds1 -> new Tuple2<>(item.name, ds1));
		});
		List<Tuple2<String, Dataset<Row>>> flat = flat(resultList);
		return flat;
	}

	public Seq<Column> convertListToSeq(List<Column> inputList) {
		return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}

	static <E> List<E> flat(Iterable<List<E>> l) {
		List<E> ll = Colls.list();
		l.forEach(l0 -> ll.addAll(l0));
		return ll;
	}

}
