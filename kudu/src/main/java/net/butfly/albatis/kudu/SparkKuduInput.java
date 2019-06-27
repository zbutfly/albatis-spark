package net.butfly.albatis.kudu;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.Desc;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.ddl.vals.ValType;
import net.butfly.albatis.spark.SparkRowInput;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.impl.Sparks;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

@Schema("kudu")
public class SparkKuduInput extends SparkRowInput {
	private static final long serialVersionUID = 5472880102313131224L;

	public SparkKuduInput(SparkSession spark, URISpec targetUri, TableDesc... table) throws IOException {
		super(spark, targetUri, null, table);
	}

	@Override
	public Map<String, String> options() {
		return Maps.of("kudu.master", targetUri.getHost());
	}

	@Override
	public String format() {
		return "org.apache.kudu.spark.kudu";
	}

	@Override
	protected List<Tuple2<String, Dataset<Row>>> load() throws KuduException {
		try (KuduClient client = new KuduClient.KuduClientBuilder(targetUri.getHost()).build();) {
			List<Tuple2<String, Dataset<Row>>> dss = Colls.list();
			for (String table : schemaAll().keySet())
				dss.add(new Tuple2<String, Dataset<Row>>(table, load1(client, table)));
			return dss;
		}
	}

	private Dataset<Row> load1(KuduClient client, String table) throws KuduException {
		Map<String, String> opts = options();
		opts.put("kudu.table", table().name);
		StructType schema = build(client, table);
		logger().debug("Loading from kudu as : " + opts + "\n\tschema: " + schema.toString());
//		TODO add time monitor
		long start = System.currentTimeMillis();
		Dataset<Row> cutedDS = spark.read().format(format()).schema(schema).options(opts).load().cache();
		logger().info("kudu cache use:"+ (System.currentTimeMillis()-start)/1000.0 + "s");
		String condition = (String) table().attr("TABLE_QUERYPARAM");
        Dataset<Row> resultDs = null;
		if (!condition.isEmpty()){
            resultDs = cutedDS.where(condition);
        }else{
		    resultDs = cutedDS;
        }
		logger().trace(() -> "Loaded from kudu, schema: " + cutedDS.schema().treeString());
		long count = resultDs.persist(StorageLevel.MEMORY_AND_DISK()).count();
		logger().info("readKudu use:\t"+ (System.currentTimeMillis()-start)/1000.0 + "s"+"\n\tcount:\t"+count+"");
		return resultDs;
	}

	private StructType build(KuduClient client, String table) throws KuduException {
		KuduTable kt = client.openTable(table);
		List<StructField> fields = Colls.list(f -> {
			String dbField = f.attr(Desc.PROJECT_FROM, f.name);
			ColumnSchema col;
			try {
				col = kt.getSchema().getColumn(dbField);
			} catch (IllegalArgumentException e) { // col not found
				logger().error("Column required [" + f.name + "] not found in kudu table [" + table + "].");
				return null;
			}
			ValType ft = KuduCommon.valType(col.getType());
			return DataTypes.createStructField(dbField, Sparks.fieldType(ft), true);
		}, schemaAll().get(table).fields());
		return DataTypes.createStructType(fields);
	}
}
