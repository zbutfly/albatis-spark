package net.butfly.albatis.hbase;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkIO.Schema;
import net.butfly.albatis.spark.io.SparkOutput;

@Schema("hbase")
public class SparkHbaseOutput extends SparkOutput {
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

	public SparkHbaseOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
		jsonCatalog = "";
	}

	@Override
	public String format() {
		return "org.apache.spark.sql.execution.datasources.hbase";
	}

	@Override
	public void process(Rmap r) {
		Mutation op = Hbases.Results.op(r, hc.conv::apply);
		try {
			hc.put(r.table(), op);
			succeeded(1);
		} catch (IOException e) {
			failed(Sdream.of1(r));
		}
	}

	@Override
	public boolean writing(long partitionId, long version) {
		try {
			hc = new HbaseConnection(targetUri);
			logger().info("Hbase native connection constructed by worker...HEAVILY!!");
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
