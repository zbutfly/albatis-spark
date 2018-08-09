package net.butfly.albatis.solr;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkIO.Schema;
import net.butfly.albatis.spark.io.SparkRmapOutput;

@Schema("solr")
public class SparkSolrOutput extends SparkRmapOutput {
	private static final long serialVersionUID = 1598463842099800246L;

	protected SparkSolrOutput(SparkSession spark, URISpec targetUri, String[] table) {
		super(spark, targetUri, table);
	}

	@Override
	public boolean enqueue(Rmap r) {
		// TODO
		return true;
	}
}
