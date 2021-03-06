package net.butfly.albatis.spark.output;

import static net.butfly.albatis.spark.impl.Schemas.rmap2row;
import static net.butfly.albatis.spark.impl.SchemaExtraField.purge;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;

class WriteHandlerStream extends WriteHandlerBase<WriteHandlerStream> {
	private DataStreamWriter<Row> writer;

	protected WriteHandlerStream(TableDesc table, Dataset<Rmap> ds) {
		super(rmap2row(table, ds));
	}

	protected WriteHandlerStream(Dataset<Row> ds) {
		super(ds);
	}

	@Override
	public void close() {
		StreamingQuery s = writer.start();
		try {
			s.awaitTermination();
		} catch (StreamingQueryException e) {
			throw new RuntimeException(e);
		}
	}

//	传入purge,进行purge处理,
	private DataStreamWriter<Row> dStreamWriter(boolean purge) {
		return (purge ? purge(ds) : ds).writeStream().outputMode(OutputMode.Update()).trigger(trigger());
	}

	@Override
	public void save(String format, Map<String, String> options) { // TODO: need two mode
		options.putIfAbsent("checkpointLocation", checkpoint());
		writer = dStreamWriter(true).format(format).options(options);
	}

	@Override
	public void save(String table, Output<Rmap> output) {
		Map<String, String> opts = Maps.of("checkpointLocation", checkpoint(), "output", output.ser(), "table", table);
		writer = dStreamWriter(false).format(SparkSinkOutput.FORMAT).options(opts);
	}

	protected Trigger trigger() {
		return Trigger.ProcessingTime(0);
	}
}
