package net.butfly.albatis.hbase;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.streaming.Sink;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.spark.util.DSdream.$utils$;

public class HbaseSink implements Sink {
	private final Map<String, String> options = Maps.of();

	public HbaseSink(scala.collection.Map<String, String> options) {
		this.options.putAll($utils$.mapizeJava(options));
		this.options.remove("uri");
	}

	@Override
	public void addBatch(long batchId, Dataset<Row> ds) {
		// Row r = ds.first();
		ds.sparkSession().createDataFrame(ds.javaRDD(), ds.schema())//
				.write().options(options).format("org.apache.spark.sql.execution.datasources.hbase").save();
	}
}
