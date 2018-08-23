package net.butfly.albatis.spark.output;

import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.OddOutput;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;

/**
 * Streaming sink writing to another traditional output or consumer
 */
public final class SparkSinkOutput extends SparkSinkOutputBase {
	private static final long serialVersionUID = 4255412656890822551L;
	public static final String FORMAT = OutputSinkProvider.class.getName();
	protected final Output<Rmap> output;

	public SparkSinkOutput(SparkSession spark, Output<Rmap> output) {
		super(spark, output.target(), output.schemaAll().values().toArray(new TableDesc[0]));
		this.output = output;
	}

	public SparkSinkOutput(SparkSession spark, Consumer<Rmap> eaching) {
		this(spark, (OddOutput<Rmap>) v -> {
			eaching.accept(v);
			return true;
		});
	}

	@Override
	public void save(Dataset<Row> ds) {
		logger().info("Dataset [" + ds.toString() + "] streaming sink to: " + output.name());
		WriteHandler.save(ds, output);
	}

	@Override
	public Connection connect() throws IOException {
		return output.connect();
	}
}
