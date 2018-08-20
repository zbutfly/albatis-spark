package net.butfly.albatis.elastic;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.output.SparkSinkSaveOutput;

@Schema({ "es", "elasticsearch" })
public class SparkESOutput extends SparkSinkSaveOutput {
	private static final long serialVersionUID = 2840201452393061853L;

	protected SparkESOutput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
	}

	@Override
	public String format() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> options() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void enqueue(Sdream<Rmap> r) {
		// TODO Auto-generated method stub
	}
}
