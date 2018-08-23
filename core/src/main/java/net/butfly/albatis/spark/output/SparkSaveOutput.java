package net.butfly.albatis.spark.output;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkOutput;
import static net.butfly.albatis.spark.impl.Sparks.alias;

/**
 * Writing by spark native save(), with self.format() and self.options()
 */
public abstract class SparkSaveOutput extends SparkOutput<Rmap> {
	private static final long serialVersionUID = -1L;

	protected SparkSaveOutput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
	}

	@Override
	public abstract String format();

	@Override
	public abstract Map<String, String> options(String table);

	@Override
	public final void save(Dataset<Row> ds) {
		logger().info("Dataset [" + ds.toString() + "] native save with format: " + format());
		try (WriteHandler w = WriteHandler.of(ds)) {
			w.save(format(), options(alias(ds)));
		} finally {
			logger().info("Spark saving finished.");
		}
	}
}
