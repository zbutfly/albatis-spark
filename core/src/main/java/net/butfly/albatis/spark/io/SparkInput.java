package net.butfly.albatis.spark.io;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albatis.io.Rmap;

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
		Dataset<Rmap> dds = ds.map(this::conv, $utils$.ENC_R);
		return dds;
	}

	protected Rmap conv(Row row) {
		return $utils$.rmap(table(), row);
	}

	private static final StructType schema = new StructType();
	static {
		schema.add("table", DataTypes.StringType);
		schema.add("key", DataTypes.StringType);
		schema.add("data", DataTypes.BinaryType);
	}

	// @Override
	// protected <T> void sink(Dataset<T> ds, Output<?> output) {
	// AtomicLong c = new AtomicLong();
	// super.sink(dataset.map(r -> row(r, c.incrementAndGet()), RowEncoder.apply(schema)), output);
	// }

	protected Row row(Rmap m, long c) {
		Row r = RowFactory.create(m.table(), null == m.key() ? null : m.key().toString(), BsonSerder.map(m));
		if (c % 30000 == 0) logger().trace("[" + Thread.currentThread().getName() + //
				"][" + c + "]\n\tRmap===> " + m.toString() + "\n\t Row===> " + r.toString());
		return r;
	}
}
