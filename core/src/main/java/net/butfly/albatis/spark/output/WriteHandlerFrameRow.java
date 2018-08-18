package net.butfly.albatis.spark.output;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.hzcominfo.albatis.nosql.Connection;

import net.butfly.albatis.io.Output;
import net.butfly.albatis.spark.util.DSdream;

class WriteHandlerFrameRow extends WriteHandlerBase<WriteHandlerFrameRow, Row> {
	protected WriteHandlerFrameRow(Dataset<Row> ds) {
		super(ds);
	}

	@Override
	public void save(String format, Map<String, String> options) {
		options.putIfAbsent("checkpointLocation", checkpoint());
		ds.write().format(format).options(options).save();
	}

	@Override
	public void save(Output<Row> output) {
		try (Connection cc = output.connect();) {
			output.enqueue(DSdream.of(ds));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
