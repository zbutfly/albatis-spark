package net.butfly.albatis.hbase;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.StreamSinkProvider;
import org.apache.spark.sql.streaming.OutputMode;

import scala.collection.Seq;

public class HbaseSinkProvider implements StreamSinkProvider, DataSourceRegister {
	@Override
	public String shortName() {
		return "hbase";
	}

	@Override
	public Sink createSink(SQLContext ctx, scala.collection.immutable.Map<String, String> params, Seq<String> partitionColumns,
			OutputMode outputMode) {
		return new HbaseSink(params);
	}
}
