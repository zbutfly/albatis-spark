package net.butfly.albatis.spark.io.impl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.streaming.Trigger;

import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;

abstract class WriteHandlerBase<W, T extends WriteHandlerBase<W, T>> implements WriteHandler {
	protected final Dataset<Rmap> ds;
	protected final Output<Rmap> output;
	protected W w;

	protected WriteHandlerBase(Dataset<Rmap> ds, Output<Rmap> output) {
		this.output = output;
		this.ds = ds;
		save();
	}

	protected abstract void save();

	protected Trigger trigger() {
		return Trigger.ProcessingTime(0);
	}

	protected String checkpoint() {
		return "/tmp/" + ds.sparkSession().sparkContext().appName();
	}
}
