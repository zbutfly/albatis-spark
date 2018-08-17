package net.butfly.albatis.spark.input;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;

/**
 * generally, any kafka with value of a serialized map should come from here
 */
public abstract class SparkDataInput extends SparkInput<Rmap> {
	private static final long serialVersionUID = 8309576584660953676L;

	public SparkDataInput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	protected Dataset<Rmap> load() {
		Map<String, String> opts = options();
		logger().info("Spark input [" + getClass().toString() + "] constructing: " + opts.toString());

		DataStreamReader dr = spark.readStream();
		String f = format();
		if (null != f) dr = dr.format(f);
		Dataset<Row> ds = dr.options(opts).load();

		Dataset<Rmap> dds = ds.map(r -> $utils$.rmap(table(), r), $utils$.ENC_R);
		return dds;
	}
	//
	// protected Rmap conv(Row row) {
	// return $utils$.rmap(table(), row);
	// }

	protected Row row(Rmap m, long c) {
		Row r = RowFactory.create(m.table(), null == m.key() ? null : m.key().toString(), BsonSerder.map(m));
		if (c % 30000 == 0) logger().trace("[" + Thread.currentThread().getName() + //
				"][" + c + "]\n\tRmap===> " + m.toString() + "\n\t Row===> " + r.toString());
		return r;
	}
}
