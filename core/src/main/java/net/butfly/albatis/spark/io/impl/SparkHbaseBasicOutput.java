package net.butfly.albatis.spark.io.impl;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkIO.Schema;
import net.butfly.albatis.spark.io.SparkOutput;
import net.butfly.albatis.spark.util.DSdream.$utils$;

@Schema(value = "hbase", priority = Integer.MIN_VALUE)
public class SparkHbaseBasicOutput extends SparkOutput {
	private static final long serialVersionUID = -8410386041741975726L;
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

	public SparkHbaseBasicOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
		jsonCatalog = "";
	}

	@Override
	public String format() {
		return "org.apache.spark.sql.execution.datasources.hbase";
	}

	@Override
	public void process(Rmap v) {
		saving(spark.sqlContext().createDataset(Colls.list(v), $utils$.ENC_R));
	}

	@Override
	public boolean writing(long partitionId, long version) {
		return true;
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
