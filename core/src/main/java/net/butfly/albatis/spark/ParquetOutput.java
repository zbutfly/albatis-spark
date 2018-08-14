package net.butfly.albatis.spark;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.output.SparkSaveOutput;

@Schema("parquet")
public class ParquetOutput extends SparkSaveOutput {
	private static final long serialVersionUID = -5643925927378821988L;
	private Map<String, String> opts;

	protected ParquetOutput(SparkSession spark, URISpec targetUri, String table) {
		super(spark, targetUri, table);
		if (tables.length != 1) throw new IllegalArgumentException("ParquetOutput need one table, can be =Expression");
	}

	@Override
	public String format() {
		return "parquet";
	}

	@Override
	public Map<String, String> options() {
		return opts;
	}
}
