package net.butfly.albatis.kafka;

import java.io.IOException;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.Rmap.Op;
import net.butfly.albatis.spark.impl.SparkIO.Schema;

@Deprecated
@Schema("kafka:etl")
public class SparkKafkaEtlInput extends SparkKafkaInput {
	private static final long serialVersionUID = -8077483839198954L;

	public SparkKafkaEtlInput(SparkSession spark, URISpec targetUri, TableDesc... table) throws IOException {
		super(spark, targetUri, table);
	}

	@Override
	protected Rmap kafka(Row kafka) {
		Rmap r = super.kafka(kafka);
		@SuppressWarnings("unchecked")
		Map<String, Object> value = (Map<String, Object>) r.get("value");
		String keyField = schema(r.table()).rowkey();
		String op = (String) r.get("oper_type");
		return new Rmap(r.table(), null == keyField ? null : (String) value.get(keyField), value).keyField(keyField)//
				.op(null == op ? Integer.parseInt(op) : Op.DEFAULT);
	}
}
