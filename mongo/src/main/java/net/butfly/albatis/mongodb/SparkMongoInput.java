package net.butfly.albatis.mongodb;

import static net.butfly.albatis.spark.impl.SchemaExtraField.FIELD_KEY_VALUE;
import static net.butfly.albatis.spark.impl.SchemaExtraField.FIELD_TABLE_NAME;
import static net.butfly.albatis.spark.impl.Sparks.split;
import static org.apache.spark.sql.functions.lit;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;

import net.butfly.albatis.Albatis;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.spark.SparkRowInput;
import net.butfly.albatis.spark.impl.SparkConf;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import scala.Tuple2;

@Schema("mongodb")
@SparkConf(key = "spark.mongodb.input.uri", value = "mongodb://127.0.0.1/FxxkMongoSpark.FakeCollection")
public class SparkMongoInput extends SparkRowInput implements SparkMongo {
	private static final long serialVersionUID = 2110132305482403155L;

	public SparkMongoInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
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
		List<List<Tuple2<String, Dataset<Row>>>> l = Colls.list(schemaAll().values(), t -> {
			Map<String, String> opts = options();
			opts.put("collection", t.name);
			ReadConfig rc = ReadConfig.create(opts);

			JavaMongoRDD<Document> rdd = MongoSpark.load(jsc, rc);
			if (Systems.isDebug()) {
				int limit = Integer.parseInt(Configs.gets(Albatis.PROP_DEBUG_INPUT_LIMIT, "-1")) / rdd.getNumPartitions() + 1;
				if (limit > 0) rdd = rdd.withPipeline(Colls.list(Document.parse("{ $limit: " + limit + " }")));
			}

			Dataset<Row> d = rdd.toDF();
			d = d.withColumn(FIELD_TABLE_NAME, lit(t.name)).withColumn(FIELD_KEY_VALUE, d.col("_id.oid")).withColumn("_id", d.col(
					"_id.oid"));
			return Colls.list(split(d, false), ds -> new Tuple2<>(t.name, ds.persist(StorageLevel.OFF_HEAP())));
		});
		return Colls.flat(l);
	}
}
