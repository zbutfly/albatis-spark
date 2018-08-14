package net.butfly.albatis.solr;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.output.SparkSinkSaveOutput;

@Schema("solr")
public class SparkSolrOutput extends SparkSinkSaveOutput {
	private static final long serialVersionUID = 1598463842099800246L;

	protected SparkSolrOutput(SparkSession spark, URISpec targetUri, String[] table) {
		super(spark, targetUri, table);
	}

	@Override
	public void enqueue(Sdream<Rmap> r) {
		// TODO Auto-generated method stub
	}
}
