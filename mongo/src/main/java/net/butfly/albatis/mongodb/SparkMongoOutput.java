package net.butfly.albatis.mongodb;

import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkRmapOutput;
import net.butfly.albatis.spark.io.SparkIO.Schema;

@Schema("mongodb")
public class SparkMongoOutput extends SparkRmapOutput {
	private static final long serialVersionUID = -887072515139730517L;

	private static final String writeconcern = "majority";
	private MongoSpark mongo;
	private String dbname;

	private transient MongoDatabase db;

	public SparkMongoOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
		java.util.Map<String, String> opts = options();
		logger().info("Spark output [" + getClass().toString() + "] constructing: " + opts.toString());
		mongo = MongoSpark.builder().javaSparkContext(new JavaSparkContext(spark.sparkContext())).options(opts).build();
		this.dbname = opts.get("database");
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
		String uri = targetUri.getScheme() + "://" + targetUri.getAuthority() + "/" + database;

		Map<String, String> options = Maps.of();
		options.put("uri", uri);
		options.put("database", database);
		options.put("collection", table());
		options.put("writeConcern.w", writeconcern);
		return options;
	}

	@Override
	public boolean writing(long partitionId, long version) {
		if (null == db) db = mongo.connector().mongoClientFactory().create().getDatabase(dbname);
		return true;
	}

	@Override
	public boolean enqueue(Rmap r) {
		Document doc = new Document(r);
		if (doc.containsKey("_id")) doc.remove("_id");
		db.getCollection(r.table()).insertOne(doc);
		logger().trace("inserted: " + r.toString());
		return true;
	}
}
