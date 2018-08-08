package net.butfly.albatis.hbase;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkIO.Schema;
import net.butfly.albatis.spark.io.SparkOutput;
import net.butfly.albatis.spark.util.DSdream;

@Schema(value = "hbase", priority = Integer.MAX_VALUE)
public class SparkHbaseShcOutput extends SparkOutput {
	private static final long serialVersionUID = -2791465592518498084L;
	// private final String jsonCatalog;
	private boolean fromStreaming = false;

	public SparkHbaseShcOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
		// jsonCatalog = "";
	}

	@Override
	public String format() {
		return fromStreaming ? "net.butfly.albatis.hbase.HbaseSinkProvider" : "org.apache.spark.sql.execution.datasources.hbase";
	}

	@Override
	protected Map<String, String> options() {
		return Maps.of("uri", targetUri.toString() //
		// , HBaseTableCatalog.table(), "" // table()
		// , HBaseTableCatalog.tableCatalog(), jsonCatalog //
		// ,HBaseTableCatalog.newTable(), "5"//
		);
	}

	@Override
	public void enqueue(Sdream<Rmap> s) {
		Dataset<Rmap> ds = DSdream.of(spark.sqlContext(), s).ds;
		fromStreaming = ds.isStreaming();
		streaming = saving(ds);
	}

	@Override
	public void process(Rmap v) {
		throw new UnsupportedOperationException();
	}
}
