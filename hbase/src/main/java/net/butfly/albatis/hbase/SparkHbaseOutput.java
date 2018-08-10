//package net.butfly.albatis.hbase;
//
//import java.io.IOException;
//import java.util.Map;
//
//import org.apache.hadoop.hbase.client.Mutation;
//import org.apache.spark.sql.SparkSession;
//
//import net.butfly.albacore.io.URISpec;
//import net.butfly.albacore.utils.collection.Maps;
//import net.butfly.albatis.io.Rmap;
//import net.butfly.albatis.spark.io.SparkRmapOutput;
//
//// @Schema("hbase")
//@Deprecated // (slow hbase output, work but put one by one...
//public class SparkHbaseOutput extends SparkRmapOutput {
//	private static final long serialVersionUID = -8410386041741975726L;
//	private transient HbaseConnection hc;
//
//	public SparkHbaseOutput(SparkSession spark, URISpec targetUri, String... table) {
//		super(spark, targetUri, table);
//	}
//
//	@Override
//	public boolean enqueue(Rmap r) {
//		Mutation op = Hbases.Results.op(r, hc.conv::apply);
//		try {
//			hc.put(r.table(), op);
//			return true;
//		} catch (IOException e) {
//			return false;
//		}
//	}
//
//	@Override
//	public boolean writing(long partitionId, long version) {
//		try {
//			hc = new HbaseConnection(targetUri);
//			logger().info("Hbase native connection constructed by worker...HEAVILY!!");
//			return true;
//		} catch (IOException e) {
//			return false;
//		}
//	}
//
//	@Override
//	protected Map<String, String> options() {
//		return Maps.of(//
//				"uri", targetUri.toString() //
//		// , "newTable", "5"//
//		);
//	}
//}
