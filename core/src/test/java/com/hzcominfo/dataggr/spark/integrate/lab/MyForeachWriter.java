//package com.hzcominfo.dataggr.spark.integrate.lab;
//
//import java.util.HashMap;
//import java.util.List;
//import net.butfly.albatis.io.R;
//
//import org.apache.spark.sql.ForeachWriter;
//import net.butfly.albatis.io.R;
//import org.apache.spark.sql.types.StructType;
//
//import net.butfly.albacore.utils.collection.Colls;
//
//public class MyForeachWriter extends ForeachWriter<R> {
//	private static final long serialVersionUID = -1072922526110204753L;
//	private List<R> ms = Colls.list();
//
//	public List<R> getMs() {
//		return ms;
//	}
//
//	@Override
//	public void close(Throwable arg0) {}
//
//	@Override
//	public boolean open(long arg0, long arg1) {
//		return true;
//	}
//
//	@Override
//	public void process(Row row) {
//		StructType schema = row.schema();
//		String[] fieldNames = schema.fieldNames();
//		Map<String, Object> map = new HashMap<>();
//		for (String fn : fieldNames) {
//			map.put(fn, row.getAs(fn));
//		}
//		System.out.println(map);
//		ms.add(map);
//	}
//}
