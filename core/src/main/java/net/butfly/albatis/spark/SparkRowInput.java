package net.butfly.albatis.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Configs;
import net.butfly.albatis.ddl.TableDesc;

public abstract class SparkRowInput extends SparkInput<Row> {
	private static final long serialVersionUID = -2144747945365613002L;

	protected SparkRowInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected abstract Dataset<Row> load();

	@Override
	protected final DatasetMode mode() {
		return DatasetMode.ROW;
	}

	protected double[] calcSplitWeights(long total) {
		@SuppressWarnings("deprecation")
		int split = Integer.parseInt(Configs.gets("albatis.spark.split", "-1")), count = 1;
		try {
			if (split > 0 && total > split) {
				for (long curr = total; curr > split; curr = curr / 2)
					count *= 2;
				double[] weights = new double[count];
				double w = 1.0 / count;
				for (int i = 0; i < count; i++)
					weights[i] = w;
				return weights;
			} else return new double[] { 1 };
		} finally {
			logger().info("Dataset [size: " + total + "], split into [" + count + "].");
		}
	}
}