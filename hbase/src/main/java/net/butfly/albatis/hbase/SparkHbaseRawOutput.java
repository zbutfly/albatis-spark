package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.serder.BsonSerder;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkIO.Schema;
import net.butfly.albatis.spark.io.SparkOutput;

@Schema("hbase")
public class SparkHbaseRawOutput extends SparkOutput {
	private static final long serialVersionUID = -8410386041741975726L;
	private transient HbaseConnection hc;
	/**
	 * <pre>
	{
		"table":{"namespace":"default", "name":"table1"},
		"rowkey":"key",
		"columns":{
			"col0":{"cf":"rowkey", "col":"key", "type":"string"},
			"col1":{"cf":"cf1", "col":"col1", "type":"string"}
		}
	}
	 * </pre>
	 */
	private final String jsonCatalog;

	public SparkHbaseRawOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
		jsonCatalog = "";
	}

	@Override
	public String format() {
		return "org.apache.spark.sql.execution.datasources.hbase";
	}

	@Override
	public void process(Rmap r) {
		Table tt = hc.table(r.table());
		Put put = new Put(Bytes.toBytes((String) r.key()));
		r.forEach((k, v) -> {
			byte[] cf, qf, val = (byte[]) v;
			String[] ks = k.split(":", 2);
			cf = Bytes.toBytes(ks.length == 2 ? ks[0] : "cf1");
			if (ks.length == 2) {
				cf = Bytes.toBytes(ks[0]);
				qf = Bytes.toBytes(ks[1]);
			} else {
				cf = Bytes.toBytes("cf1");
				qf = Bytes.toBytes(ks[0]);
			}
			logger().trace(() -> BsonSerder.map(val).toString());
			put.addColumn(cf, qf, val);
		});
		try {
			tt.put(put);
		} catch (IOException e) {
			logger().error("Put error", e);
		}
	}

	@Override
	public boolean writing(long partitionId, long version) {
		try {
			hc = new HbaseConnection(targetUri);
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	@Override
	protected Map<String, String> options() {
		return Maps.of(//
				"uri", targetUri.toString(), //
				"table", "", // table()
				"tableCatalog", jsonCatalog, //
				"newTable", "5");
	}
}
