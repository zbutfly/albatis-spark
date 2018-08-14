package net.butfly.albatis.mongodb;

import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.output.SparkSinkSaveOutput;
import net.butfly.albatis.spark.output.SparkWriting;

@Schema("mongodb")
public class SparkMongoOutput extends SparkSinkSaveOutput implements SparkWriting {
	private static final long serialVersionUID = -887072515139730517L;
	private static final String WRITE_CONCERN = "majority";

	private MongoSpark mongo;
	private String dbname;

	public SparkMongoOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
		java.util.Map<String, String> opts = options();
		logger().info("Spark output [" + getClass().toString() + "] constructing: " + opts.toString());
		mongo = MongoSpark.builder().javaSparkContext(new JavaSparkContext(spark.sparkContext())).options(opts).build();
		this.dbname = opts.get("databaseName");
	}

	@Override
	public void close() {
		super.close();
	}

	@Override
	public Map<String, String> options() {
		String[] path = targetUri.getPaths();
		if (path.length != 1) throw new RuntimeException("Mongodb URI is incorrect");
		String database = path[0];

		return Maps.of("databaseName", database, //
				"collectionName", table(), //
				"replaceDocument", true, //
				"maxBatchSize", 200, //
				"localThreshold", "0", //
				"writeConcern", WRITE_CONCERN);
	}

	@Override
	public boolean writing(long partitionId, long version) {
		return true;
	}

	@Override
	public void enqueue(Sdream<Rmap> s) {
		// DataFrameWriter<Rmap> w = ds.write().option("collection", "hundredClub").mode(SaveMode.Overwrite);
		// MongoSpark.save(ds, WriteConfig.create(options()));
		try (MongoClient m = mongo.connector().mongoClientFactory().create();) {
			MongoDatabase db = m.getDatabase(dbname);
			// TODO: optimizing to batch insert
			s.eachs(r -> {
				Document doc = new Document(r);
				if (doc.containsKey("_id")) doc.remove("_id");
				db.getCollection(r.table()).insertOne(doc);
				logger().trace("inserted: " + r.toString());
			});
		}
	}
}
