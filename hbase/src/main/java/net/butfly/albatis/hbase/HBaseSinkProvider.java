package net.butfly.albatis.hbase;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.StreamSinkProvider;
import org.apache.spark.sql.streaming.OutputMode;

import scala.collection.Seq;
import scala.collection.immutable.Map;

public class HBaseSinkProvider implements StreamSinkProvider, DataSourceRegister {
	@Override
	public String shortName() {
		return "hbase";
	}

	@Override
	public Sink createSink(SQLContext ctx, Map<String, String> params, Seq<String> partitionColumns, OutputMode outputMode) {
		return new HBaseSink(params);
	}

}
