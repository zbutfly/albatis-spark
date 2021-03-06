package net.butfly.albatis.spark.output;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;

public interface WriteHandler extends AutoCloseable {
	// /**
	// * Foreach writing (streaming by sink or stocking)<br>
	// * Sink writing by <code>OutputSink</code>, <code>uri</code> in <code>options()</code> is required.
	// */
	// public static void save(Dataset<Row> ds, Output<Rmap> output) {
	// try (WriteHandler w = ds.isStreaming() ? new WriteHandlerStream(ds, output) : new WriteHandlerFrame(ds, output);) {}
	// }

	@Override
	default void close() {}

	void save(String table, Output<Rmap> output);

	void save(String format, Map<String, String> options);

	static WriteHandler of(TableDesc table, Dataset<Rmap> ds) {
		if (ds.isStreaming()) return new WriteHandlerStream(table, ds);
		else return new WriteHandlerFrame(table, ds);
	}

	static WriteHandler of(Dataset<Row> ds) {
		if (ds.isStreaming()) return new WriteHandlerStream(ds);
		else return new WriteHandlerFrame(ds);
	}
}
