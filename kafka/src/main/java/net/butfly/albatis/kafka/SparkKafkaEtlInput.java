package net.butfly.albatis.kafka;

import static net.butfly.albatis.spark.impl.Sparks.SchemaSupport.ROW_KEY_FIELD_FIELD;
import static net.butfly.albatis.spark.impl.Sparks.SchemaSupport.ROW_KEY_VALUE_FIELD;
import static net.butfly.albatis.spark.impl.Sparks.SchemaSupport.ROW_OP_FIELD;
import static net.butfly.albatis.spark.impl.Sparks.SchemaSupport.ROW_TABLE_NAME_FIELD;

import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap.Op;
import net.butfly.albatis.spark.impl.SparkIO.Schema;

@Deprecated
@Schema("kafka:etl")
public class SparkKafkaEtlInput extends SparkKafkaInput {
	private static final long serialVersionUID = -8077483839198954L;

	public SparkKafkaEtlInput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
	}

	@Override
	protected Map<String, Object> kafka(Row kafka) {
		Map<String, Object> map = super.kafka(kafka);
		@SuppressWarnings("unchecked")
		Map<String, Object> value = (Map<String, Object>) map.get("value");
		String topic = (String) map.get(ROW_TABLE_NAME_FIELD);

		String keyField = schema(topic).rowkey();
		String keyValue = null == keyField ? null : (String) value.get(keyField);
		String op = (String) map.get("oper_type");
		int opv = null == op ? Integer.parseInt(op) : Op.DEFAULT;

		value.put(ROW_KEY_FIELD_FIELD, keyField);
		value.put(ROW_KEY_VALUE_FIELD, keyValue);
		value.put(ROW_OP_FIELD, opv);
		return value;
	}
}
