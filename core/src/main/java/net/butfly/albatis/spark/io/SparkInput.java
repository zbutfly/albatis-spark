package net.butfly.albatis.spark.io;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.util.DSdream.$utils$;

/**
 * generally, any kafka with value of a serialized map should come from here
 */
public abstract class SparkInput extends SparkInputBase<Rmap> {
	private static final long serialVersionUID = 8309576584660953676L;

	public SparkInput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	protected Dataset<Rmap> load() {
		Map<String, String> opts = options();
		logger().info("Spark input [" + getClass().toString() + "] constructing: " + opts.toString());
		Dataset<Row> ds = spark.readStream().format(format()).options(opts).load();
		// dds.printSchema();
		Dataset<Rmap> dds = ds.map(this::conv, $utils$.ENC_R);
		return dds;
	}

	protected Rmap conv(Row row) {
		return new Rmap(table(), $utils$.rowMap(row));
	}
}
