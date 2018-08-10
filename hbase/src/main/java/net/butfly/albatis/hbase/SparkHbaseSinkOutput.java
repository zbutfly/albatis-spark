//package net.butfly.albatis.hbase;
//
//import java.util.Map;
//
//import org.apache.spark.sql.SparkSession;
//
//import net.butfly.albacore.io.URISpec;
//import net.butfly.albacore.paral.Sdream;
//import net.butfly.albacore.utils.collection.Maps;
//import net.butfly.albatis.io.Rmap;
//import net.butfly.albatis.spark.io.SparkRmapOutput;
//
//// @Schema(value = "hbase", priority = Integer.MAX_VALUE)
//@Deprecated // (work well on testing, not fully implemented since we use general sink, not only for hbase, but for any output
//public class SparkHbaseSinkOutput extends SparkRmapOutput {
//	private static final long serialVersionUID = -2791465592518498084L;
//	// private final String jsonCatalog;
//
//	public SparkHbaseSinkOutput(SparkSession spark, URISpec targetUri, String... table) {
//		super(spark, targetUri, table);
//		// jsonCatalog = "";
//	}
//
//	@Override
//	protected Map<String, String> options() {
//		return Maps.of("uri", targetUri.toString() //
//		// , HBaseTableCatalog.table(), "" // table()
//		// , HBaseTableCatalog.tableCatalog(), jsonCatalog //
//		// ,HBaseTableCatalog.newTable(), "5"//
//		);
//	}
//
//	@Override
//	public void enqueue(Sdream<Rmap> s) {
//		// TODO: write into hbase
//	}
//
//	@Override
//	public boolean enqueue(Rmap r) {
//		throw new UnsupportedOperationException();
//	}
//}
