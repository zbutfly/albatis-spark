package net.butfly.albatis.solr;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkIO.Schema;
import net.butfly.albatis.spark.io.SparkOutput;

@Schema("solr")
public class SparkSolrOutput extends SparkOutput {
	private static final long serialVersionUID = 1598463842099800246L;

	protected SparkSolrOutput(SparkSession spark, URISpec targetUri, String[] table) {
		super(spark, targetUri, table);
	}

	@Override
	public void process(Rmap v) {}
}
