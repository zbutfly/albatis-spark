package net.butfly.albatis.mongodb;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.input.SparkDataInput;

@Schema("mongodb")
public class SparkMongoInput extends SparkDataInput implements SparkMongo {
	private static final long serialVersionUID = 2110132305482403155L;

	public SparkMongoInput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	public Map<String, String> options() {
		Map<String, String> opts = mongoOpts(targetUri);
		opts.put("partitioner", "MongoSamplePartitioner");
		return opts;
	}

	@Override
	protected Dataset<Rmap> load() {
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		Dataset<Rmap> dds = null;
		for (String t : tables) {
			Map<String, String> opts = options();
			opts.put("collection", t);
			ReadConfig rc = ReadConfig.create(opts);
			JavaMongoRDD<Document> rdd = MongoSpark.load(jsc, rc);
			if (Systems.isDebug()) {
				int limit = Integer.parseInt(System.getProperty("albatis.spark.mongodb.input.debug.limit", "-1"));
				if (limit > 0) {
					logger().error("Debugging, resultset is limit as [" + limit + "] by setting \"albatis.spark.mongodb.input.limit\".");
					rdd = rdd.withPipeline(Colls.list(Document.parse("{ $limit: " + limit + " }")));
				} else logger().info(
						"Debugging, resultset can be limited as setting \"albatis.spark.mongodb.input.limit\", if presented and positive.");

			}
			// rdd.first();
			Dataset<Row> ds = unwrapId(rdd.toDF());
			Dataset<Rmap> dsr = ds.map(r -> $utils$.rmap(table(), r), $utils$.ENC_R);
			dds = null == dds ? dsr : dds.union(dsr);
		}
		return dds;
	}

	private Dataset<Row> unwrapId(Dataset<Row> ds) {
		StructType s = ds.schema();
		int d;
		try {
			d = ds.schema().fieldIndex("_id");
		} catch (IllegalArgumentException ex) {
			return ds;
		}
		List<StructField> fs = Colls.list(s.fields());
		StructField f = fs.remove(d);
		fs.add(new StructField("_id", DataTypes.StringType, f.nullable(), f.metadata()));
		StructType sch = new StructType(fs.toArray(new StructField[fs.size()]));
		return ds.map(r -> {
			Object[] values = $utils$.listJava(r.toSeq()).toArray();
			values[d] = ((Row) r.get(d)).getAs("oid");
			return new GenericRowWithSchema(values, sch);
		}, RowEncoder.apply(sch));
	}
}
