//package com.hzcominfo.dataggr.spark.integrate.lab;
//
//import java.io.Serializable;
//import java.util.HashMap;
//import java.util.List;
//import net.butfly.albatis.io.R;
//import java.util.function.Consumer;
//
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.ForeachWriter;
//import net.butfly.albatis.io.R;
//import org.apache.spark.sql.streaming.DataStreamWriter;
//import org.apache.spark.sql.types.StructType;
//
//import com.hzcominfo.dataggr.spark.io.SparkConnection;
//
//import net.butfly.albacore.io.URISpec;
//import net.butfly.albacore.paral.Sdream;
//import net.butfly.albacore.utils.collection.Colls;
//import net.butfly.albatis.io.Input;
//import net.butfly.albatis.io.Map<String, Object>;
//
///**
// * test
// * 
// * @author chenw
// *
// */
//public class SparkFeatureInput2 implements Input<R>, Serializable {
//	private static final long serialVersionUID = -4742008582795468309L;
//	private final SparkConnection client;
//	private final Dataset<R> dataset;
//
//	public SparkFeatureInput2(String name, String bootstrapUrl) {
//		this(name, new URISpec(bootstrapUrl));
//	}
//
//	public SparkFeatureInput2(String name, URISpec uriSpec) {
//		client = new SparkConnection(name, uriSpec);
//		dataset = client.dequeue();
//		closing(this::close);
//	}
//
//	private static interface Writing extends Consumer<Sdream<R>>, Serializable {}
//
//	private class ForeachWriter$anonfun$ extends ForeachWriter<R> implements Serializable {
//		private static final long serialVersionUID = -6782476040095757847L;
//
//		public ForeachWriter$anonfun$(Consumer<Sdream<R>> using) {
//			super();
//			this.using = using::accept;
//		}
//
//		private Writing using;
//
//		@Override
//		public void close(Throwable arg0) {}
//
//		@Override
//		public boolean open(long arg0, long arg1) {
//			return true;
//		}
//
//		@Override
//		public void process(Row row) {
//			StructType schema = row.schema();
//			String[] fieldNames = schema.fieldNames();
//			Map<String, Object> map = new HashMap<>();
//			for (String fn : fieldNames) {
//				map.put(fn, row.getAs(fn));
//			}
//			Map<String, Object> message = new Map<String, Object>(map);
//			List<R> ms = Colls.list();
//			ms.add(message);
//			using.accept(Sdream.of(ms));
//		}
//	}
//
//	@Override
//	public void dequeue(Consumer<Sdream<R>> using) {
//		if (dataset == null) return;
//
//		if (dataset.isStreaming()) {
//			DataStreamWriter<R> s = dataset.writeStream();
//			ForeachWriter$anonfun$ fw = new ForeachWriter$anonfun$((Consumer<Sdream<R>> & Serializable) using::accept);
//			s.foreach(fw);
//		} else {
//			dataset.foreach(row -> {
//				StructType schema = row.schema();
//				String[] fieldNames = schema.fieldNames();
//				Map<String, Object> map = new HashMap<>();
//				for (String fn : fieldNames) {
//					map.put(fn, row.getAs(fn));
//				}
//				System.out.println(map); //
//				Map<String, Object> message = new Map<String, Object>(map);
//				List<R> ms = Colls.list();
//				ms.add(message);
//				using.accept(Sdream.of(ms));
//			});
//		}
//
//	}
//
//	@Override
//	public void close() {
//		client.close();
//	}
//}
