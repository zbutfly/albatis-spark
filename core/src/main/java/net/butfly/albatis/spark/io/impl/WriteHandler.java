package net.butfly.albatis.spark.io.impl;

import org.apache.spark.sql.Dataset;

import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;

public interface WriteHandler extends AutoCloseable {
	/**
	 * Foreach writing (streaming by sink or stocking)<br>
	 * Sink writing by <code>OutputSink</code>, <code>uri</code> in <code>options()</code> is required.
	 */
	public static void save(Dataset<Rmap> ds, Output<Rmap> output) {
		try (WriteHandler w = ds.isStreaming() ? new WriteHandlerStream(ds, output) : new WriteHandlerFrame(ds, output);) {}
	}

	@Override
	default void close() {}
}
