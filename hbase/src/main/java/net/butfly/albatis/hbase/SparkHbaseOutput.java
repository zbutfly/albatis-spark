package net.butfly.albatis.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.io.SparkIO.Schema;
import net.butfly.albatis.spark.io.SparkOutput;

@Schema("hbase")
public class SparkHbaseOutput extends SparkOutput<Dataset<Rmap>> {
	private static final long serialVersionUID = -2791465592518498084L;

	public SparkHbaseOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
	}

	@Override
	public void close() {
		super.close();
	}

	@Override
	protected java.util.Map<String, String> options() {
		String[] path = targetUri.getPaths();
		if (path.length != 1) throw new RuntimeException("Mongodb URI is incorrect");
		String database = path[0];
		String uri = targetUri.getScheme() + "://" + targetUri.getAuthority() + "/" + database;

		java.util.Map<String, String> options = Maps.of();
		options.put("uri", uri);
		options.put("database", database);
		options.put("collection", table());
		return options;
	}

	@Override
	public boolean enqueue(Rmap row) {
		inputDF.
		   writeStream.
		   queryName("hbase writer").
		   format("HBase.HBaseSinkProvider").
		   option("checkpointLocation", checkPointProdPath).
		   option("hbasecat", catalog).
		   outputMode(OutputMode.Update()).
		   trigger(Trigger.ProcessingTime(30.seconds)).
		   start;		
		return true;
	}

	static class HBaseForeachWriter extends ForeachWriter<Rmap> {
		private static final long serialVersionUID = 2347534935303821198L;
		public final String tableName;
		public final String[] hbaseConfResources;

		HBaseForeachWriter() {
			this.tableName = "hbase-table-name";
			// your cluster files, i assume here it is in resources
			this.hbaseConfResources = new String[] { "core-site.xml", "hbase-site.xml" };
		}

		Put toPut(Rmap record) {
			String key = "";
			String columnFamaliyName = "";
			String columnName = "";
			String columnValue = "";

			Put p = new Put(Bytes.toBytes(key));
			// Add columns ...
			p.addColumn(Bytes.toBytes(columnFamaliyName), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
			return p;
		}

		@Override
		public void close(Throwable arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		public boolean open(long arg0, long arg1) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void process(Rmap arg0) {
			// TODO Auto-generated method stub

		}
	}
}
