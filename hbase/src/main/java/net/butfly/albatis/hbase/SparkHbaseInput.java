package net.butfly.albatis.hbase;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.spark.io.SparkIO.Schema;
import net.butfly.albatis.spark.io.SparkInput;

@Schema("hbase")
public class SparkHbaseInput extends SparkInput {
	private static final long serialVersionUID = 2274820879371771431L;

	public SparkHbaseInput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	protected java.util.Map<String, String> options() {
		String[] path = targetUri.getPaths();
		if (path.length != 1) throw new RuntimeException("Mongodb uriSpec is incorrect");
		String database = path[0];
		String uri = targetUri.getScheme() + "://" + targetUri.getAuthority() + "/" + database;

		java.util.Map<String, String> options = Maps.of();
		options.put("partitioner", "MongoSamplePartitioner");
		options.put("uri", uri);
		options.put("database", database);
		options.put("collection", table());
		return options;
	}
}
