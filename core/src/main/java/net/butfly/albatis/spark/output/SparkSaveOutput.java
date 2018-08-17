package net.butfly.albatis.spark.output;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkOutput;

/**
 * Writing by spark native save(), with self.format() and self.options()
 */
public abstract class SparkSaveOutput extends SparkOutput<Rmap> {
	private static final long serialVersionUID = -1L;

	protected SparkSaveOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	public abstract String format();

	@Override
	public abstract Map<String, String> options();

	@Override
	public final void save(Dataset<Rmap> ds) {
		logger().info("Dataset [" + ds.toString() + "] native save with format: " + format());
		if (!schema().isEmpty()) {
			Dataset<Row> d = rowDSWithoutRmap(ds);
			try (WriteHandler<Row> w = WriteHandler.ofRow(d)) {
				w.save(format(), options());
			}
		}
	}
}
