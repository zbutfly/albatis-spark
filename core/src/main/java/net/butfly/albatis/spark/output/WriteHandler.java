package net.butfly.albatis.spark.output;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;

public interface WriteHandler<R> extends AutoCloseable {
	/**
	 * Foreach writing (streaming by sink or stocking)<br>
	 * Sink writing by <code>OutputSink</code>, <code>uri</code> in <code>options()</code> is required.
	 */
	// public static void save(Dataset<Rmap> ds, Output<Rmap> output) {
	// try (WriteHandler w = ds.isStreaming() ? new WriteHandlerStream(ds, output) : new WriteHandlerFrame(ds, output);) {}
	// }

	@Override
	default void close() {}

	void save(Output<R> output);

	void save(String format, Map<String, String> options);

	static WriteHandler<Rmap> of(Dataset<Rmap> ds) {
		if (ds.isStreaming()) return new WriteHandlerStream(ds);
		else return new WriteHandlerFrame(ds);
	}

	static WriteHandler<Row> ofRow(Dataset<Row> ds) {
		if (ds.isStreaming()) return new WriteHandlerStreamRow(ds);
		else return new WriteHandlerFrameRow(ds);
	}
}
