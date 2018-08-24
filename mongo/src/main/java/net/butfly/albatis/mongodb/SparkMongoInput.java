package net.butfly.albatis.mongodb;

import static net.butfly.albatis.spark.impl.Schemas.ROW_KEY_VALUE_FIELD;
import static net.butfly.albatis.spark.impl.Schemas.ROW_TABLE_NAME_FIELD;
import static org.apache.spark.sql.functions.lit;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.spark.SparkRowInput;
import net.butfly.albatis.spark.impl.SparkIO.Schema;

@Schema("mongodb")
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
	protected List<Dataset<Row>> load() {
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		List<Dataset<Row>> dds = Colls.list();
		for (TableDesc t : schemaAll().values()) {
			Map<String, String> opts = options();
			opts.put("collection", t.name);
			ReadConfig rc = ReadConfig.create(opts);

			JavaMongoRDD<Document> rdd = MongoSpark.load(jsc, rc);
			if (Systems.isDebug()) {
				@SuppressWarnings("deprecation")
				int limit = Integer.parseInt(Configs.gets("albatis.spark.debug.limit", "-1")) / rdd.getNumPartitions() + 1;
				if (limit > 0) rdd = rdd.withPipeline(Colls.list(Document.parse("{ $limit: " + limit + " }")));
			}

			Dataset<Row> d = rdd.toDF();
			d = d.withColumn(ROW_TABLE_NAME_FIELD, lit(t.name)).withColumn(ROW_KEY_VALUE_FIELD, d.col("_id.oid")).withColumn("_id", d.col(
					"_id.oid"));
			double[] w = calcSplitWeights(d.count());
			// if (w.length > 1)
			// Dataset<Row>[] dss = d.randomSplit(w);

			d = d.persist(StorageLevel.OFF_HEAP());
			dds = null == dds ? d : dds.union(d);
		}
		return dds;
	}
}
