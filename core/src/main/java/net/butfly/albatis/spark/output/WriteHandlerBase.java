package net.butfly.albatis.spark.output;

import org.apache.spark.sql.Dataset;

import net.butfly.albatis.io.Rmap;

abstract class WriteHandlerBase<T extends WriteHandlerBase<T>> implements WriteHandler {
	protected final Dataset<Rmap> ds;

	protected WriteHandlerBase(Dataset<Rmap> ds) {
		this.ds = ds;
	}

	protected String checkpoint() {
		return "/tmp/" + ds.sparkSession().sparkContext().appName();
	}
}
