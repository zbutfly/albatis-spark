package net.butfly.albatis.spark.impl;

import static net.butfly.albatis.spark.impl.Sparks.fieldType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.io.Rmap;

public final class SchemaExtraField implements Serializable {
	private static final long serialVersionUID = 5516039346216373689L;
	public final static String FIELD_TABLE_NAME = "___table";
	public final static String FIELD_TABLE_EXPR = "___table_expr";
	public final static String FIELD_KEY_VALUE = "___key_value";
	public final static String FIELD_KEY_FIELD = "___key_field";
	public final static String FIELD_OP = "___op";
	public static final Map<String, SchemaExtraField> FIELDS = Maps.of();
	public static final List<String> FIELDS_LIST = Colls.list();
	static {
		add(FIELD_TABLE_NAME, Rmap::table);
		add(FIELD_TABLE_EXPR, Rmap::tableExpr);
		add(FIELD_KEY_VALUE, Rmap::key);
		add(FIELD_KEY_FIELD, Rmap::keyField);
		add(FIELD_OP, DataTypes.IntegerType, Rmap::op);
	}

	final StructField struct;
	final Function<Rmap, ?> getter;

	public static Dataset<Row> purge(Dataset<Row> ds) {
		Dataset<Row> d = ds;
		for (String f : FIELDS_LIST)
			if (ds.schema().getFieldIndex(f).nonEmpty()) d = d.drop(ds.col(f));
		return d;
	}

	public static SchemaExtraField get(String name) {
		return FIELDS.get(name);
	}

	public static SchemaExtraField get(int i) {
		return i >= 0 && i < FIELDS_LIST.size() ? FIELDS.get(FIELDS_LIST.get(i)) : null;
	}

	private static void add(String name, Function<Rmap, ?> getter) {
		add(name, DataTypes.StringType, getter);
	}

	private static void add(String name, DataType type, Function<Rmap, ?> getter) {
		FIELDS.put(name, new SchemaExtraField(name, type, getter));
		FIELDS_LIST.add(name);
	}

	private SchemaExtraField(String name, DataType type, Function<Rmap, ?> getter) {
		this(createStructField(name, type, true), getter);
	}

	private SchemaExtraField(StructField field, Function<Rmap, ?> getter) {
		this.struct = field;
		this.getter = getter;
	}

	static StructField struct(FieldDesc f) {
		return createStructField(f.name, fieldType(f.type), true);
	}
}
