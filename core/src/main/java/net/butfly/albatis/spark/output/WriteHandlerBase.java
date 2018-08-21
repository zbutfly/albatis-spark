package net.butfly.albatis.spark.output;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import net.butfly.albatis.spark.impl.Sparks;

abstract class WriteHandlerBase<T extends WriteHandlerBase<T>> implements WriteHandler {
	protected final Dataset<Row> ds;

	protected WriteHandlerBase(Dataset<Row> ds) {
		this.ds = ds;
	}

	protected String checkpoint() {
		return "/tmp/" + ds.sparkSession().sparkContext().appName();
	}

	protected Dataset<Row> purge() {
		Dataset<Row> d = ds;
		for (StructField f : Sparks.SchemaSupport.EXTRA_FIELDS_SCHEMA)
			if (ds.schema().getFieldIndex(f.name()).nonEmpty()) d = d.drop(ds.col(f.name()));
		return d;
	}
}