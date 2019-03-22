package net.butfly.albatis.spark.output;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

abstract class WriteHandlerBase<T extends WriteHandlerBase<T>> implements WriteHandler {
	protected final Dataset<Row> ds;

	protected WriteHandlerBase(Dataset<Row> ds) {
		this.ds = ds;
	}

	protected String checkpoint() {
//		TODO if yarn mode,should use hdfs path.eg: hdfs://nameservice1/tmp/xxx
		return "/tmp/" + ds.sparkSession().sparkContext().appName();
	}
}
