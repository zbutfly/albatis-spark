package net.butfly.albatis.spark.output;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Rmap;

/**
 * Writing by sink into batch and then call self.enqueue()
 */
public abstract class SparkSinkSaveOutput extends SparkSinkOutputBase {
	private static final long serialVersionUID = 1L;

	public SparkSinkSaveOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	public final void save(Dataset<Rmap> ds) {
		logger().info("Dataset [" + ds.toString() + "] save by sinking into batchs.");
		try (WriteHandler w = WriteHandler.of(ds)) {
			w.save(this);
		}
	}

	@Override
	public abstract void enqueue(Sdream<Rmap> r);
}
