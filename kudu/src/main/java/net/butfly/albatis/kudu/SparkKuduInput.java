package net.butfly.albatis.kudu;

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
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.ddl.vals.ValType;
import net.butfly.albatis.spark.SparkRowInput;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.impl.Sparks;
import scala.Tuple2;

@Schema("kudu")
public class SparkKuduInput extends SparkRowInput {
	private static final long serialVersionUID = 5472880102313131224L;

	public SparkKuduInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
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
	protected List<Tuple2<String, Dataset<Row>>> load() {

		return Colls.list(schemaAll().keySet(), table -> {
			return new Tuple2<String, Dataset<Row>>(table, //
					load1(table));
		});
	}

	private Dataset<Row> load1(String table) {
		Map<String, String> opts = options();
		opts.put("kudu.table", table().name);
		return spark.read().format(format()).schema(build(opts.get("kudu.master"), table)).options(opts).load();
	}

	private StructType build(String master, String table) {
		try (KuduClient client = new KuduClient.KuduClientBuilder(master).build();) {
			KuduTable kt = client.openTable(table);
			List<StructField> fields = Colls.list(f -> {
				ColumnSchema col;
				try {
					col = kt.getSchema().getColumn(f.name);
				} catch (IllegalArgumentException e) { // col not found
					logger().error("Column required [" + f.name + "] not found in kudu table [" + table + "].");
					return null;
				}
				ValType ft = KuduCommon.valType(col.getType());
				return DataTypes.createStructField(f.name, Sparks.fieldType(ft), true);
			}, schemaAll().get(table).fields());
			return DataTypes.createStructType(fields);
		} catch (KuduException e) {
			throw new RuntimeException(e);
		}
	}
}
