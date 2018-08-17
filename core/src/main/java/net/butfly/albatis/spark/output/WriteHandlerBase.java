package net.butfly.albatis.spark.output;

import org.apache.spark.sql.Dataset;

abstract class WriteHandlerBase<T extends WriteHandlerBase<T, R>, R> implements WriteHandler<R> {
	protected final Dataset<R> ds;

	protected WriteHandlerBase(Dataset<R> ds) {
		this.ds = ds;
	}

	protected String checkpoint() {
		return "/tmp/" + ds.sparkSession().sparkContext().appName();
	}
}
