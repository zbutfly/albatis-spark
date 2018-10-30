package net.butfly.albatis.spark.output;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;

/**
 * Writing by sink into batch and then call self.enqueue()
 */
public abstract class SparkSinkSaveOutput extends SparkSinkOutputBase {
	private static final long serialVersionUID = 1L;

	public SparkSinkSaveOutput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
	}

	@Override
	public final void save(String table, Dataset<Row> ds) {
		logger().info("Dataset [" + ds.toString() + "] saving into output [" + getClass().getSimpleName() + "].");
		try (WriteHandler w = WriteHandler.of(ds)) {
			w.save(table, this);
		}
	}

	@Override
	public abstract void enqueue(Sdream<Rmap> r);
}
