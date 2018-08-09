//package net.butfly.albatis.hbase;
//
//import java.io.IOException;
//
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.execution.streaming.Sink;
//import org.apache.spark.sql.sources.DataSourceRegister;
//import org.apache.spark.sql.sources.StreamSinkProvider;
//import org.apache.spark.sql.sources.v2.DataSourceV2;
//import org.apache.spark.sql.streaming.OutputMode;
//
//import net.butfly.albatis.spark.io.SparkIO.$utils$;
//import scala.collection.Seq;
//
//public class HbaseSinkProvider implements DataSourceV2, /* CreatableRelationProvider, */StreamSinkProvider, DataSourceRegister {
//	@Override
//	public String shortName() {
//		return "hbase";
//	}
//
//	@Override
//	public Sink createSink(SQLContext ctx, scala.collection.immutable.Map<String, String> options, Seq<String> partitionColumns,
//			OutputMode outputMode) {
//		try {
//			return new HbaseSink($utils$.mapizeJava(options));
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//	}
//	//
//	// @Override
//	// public BaseRelation createRelation(SQLContext ctx, SaveMode saveMode, Map<String, String> options, Dataset<Row> data) {
//	// java.util.Map<String, String> opts = $utils$.mapizeJava(options);
//	// // from ConsoleSinkProvider
//	// int numRowsToShow = Integer.parseInt(opts.getOrDefault("numRows", "20"));
//	//
//	// // Truncate the displayed data if it is too long, by default it is true
//	// boolean isTruncated = Boolean.parseBoolean(opts.getOrDefault("truncate", "true"));
//	// data.show(numRowsToShow, isTruncated);
//	// return new HbaseRelation(ctx, data);
//	// }
//}
