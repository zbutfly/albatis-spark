package com.hzcominfo.dataggr.spark.integrate.mongo;

import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.hzcominfo.dataggr.spark.io.SparkIO.Schema;
import com.hzcominfo.dataggr.spark.io.SparkOutput;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.R;

@Schema("mongodb")
public class SparkMongoOutput extends SparkOutput<R> {
	private static final long serialVersionUID = -887072515139730517L;

	private static final String writeconcern = "majority";
	private MongoSpark mongo;
	private String dbName;

	private transient MongoDatabase db;

	public SparkMongoOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
		java.util.Map<String, String> opts = options();
		logger().info("Spark output [" + getClass().toString() + "] constructing: " + opts.toString());
		mongo = MongoSpark.builder().javaSparkContext(jsc()).options(opts).build();
		this.dbName = opts.get("database");
	}

	@Override
	public void close() {
		super.close();
	}

	@Override
	protected java.util.Map<String, String> options() {
		String[] path = targetUri.getPaths();
		if (path.length != 1) throw new RuntimeException("Mongodb URI is incorrect");
		String database = path[0];
		String uri = targetUri.getScheme() + "://" + targetUri.getAuthority() + "/" + database;

		java.util.Map<String, String> options = Maps.of();
		options.put("uri", uri);
		options.put("database", database);
		options.put("collection", table());
		options.put("writeConcern.w", writeconcern);
		return options;
	}

	@Override
	public boolean enqueue(R row) {
		if (null == db) db = mongo.connector().mongoClientFactory().create().getDatabase(dbName);
		Document doc = new Document(row);
		if (doc.containsKey("_id")) doc.remove("_id");
		db.getCollection(row.table()).insertOne(doc);
		logger().trace("inserted: " + row.toString());
		return true;
	}
}
