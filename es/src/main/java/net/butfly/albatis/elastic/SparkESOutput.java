package net.butfly.albatis.elastic;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkRmapOutput;
import net.butfly.albatis.spark.io.SparkIO.Schema;

@Schema({ "es", "elasticsearch" })
public class SparkESOutput extends SparkRmapOutput {
	private static final long serialVersionUID = 2840201452393061853L;

	protected SparkESOutput(SparkSession spark, URISpec targetUri, String[] table) {
		super(spark, targetUri, table);
	}

	@Override
	public boolean enqueue(Rmap r) {
		// TODO
		return true;
	}
}
