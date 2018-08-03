//package com.hzcominfo.dataggr.spark.util;
//
//import java.io.Serializable;
//import java.util.function.Consumer;
//import java.util.function.Function;
//
//import org.apache.spark.sql.ForeachWriter;
//import net.butfly.albatis.io.R;
//
//public interface RowConsumer extends Consumer<R>, Serializable {
//	default ForeachWriter<R> writer() {
//		return new ForeachWriter<R>() {
//			private static final long serialVersionUID = 3602739322755312373L;
//
//			@Override
//			public void process(Row r) {
//				accept(r);
//			}
//
//			@Override
//			public boolean open(long partitionId, long version) {
//				return true;
//			}
//
//			@Override
//			public void close(Throwable err) {}
//		};
//	}
//
//	interface RowConv extends Function<Row, Row>, Serializable {}
//
//	default RowConsumer before(RowConv conv) {
//		return r -> accept(conv.apply(r));
//	}
//}
