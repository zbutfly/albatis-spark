package com.hzcominfo.dataggr.spark.io;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.OddInput;
import net.butfly.albatis.io.OddOutput;

public abstract class SparkInput extends SparkIO implements OddInput<Row>, Serializable {
	private static final long serialVersionUID = 6966901980613011951L;
	private static final Logger logger = Logger.getLogger(SparkInput.class);

	private transient Dataset<Row> dataset;
	private transient StreamingQuery streaming = null;

	public SparkInput() {
		super();
	}

	protected SparkInput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}
	
	@Override
	public void open() {
		OddInput.super.open();
		dataset = load();
	}

	protected abstract Dataset<Row> load();

	@Override
	public void close() {
		OddInput.super.close();
		spark.close();
	}

	@Override
	public Row dequeue() {
		throw new UnsupportedOperationException();
		// using.accept(conv(dataset));
	}

	public final Dataset<Row> dataset() {
		return dataset;
	}

	public SparkPump pump(OddOutput<Row> output) {
		return new SparkPump(this, output);
	}

	void start(OddOutput<Row> output) {
		if (dataset.isStreaming()) streaming = dataset.writeStream().foreach(new ForeachWriter<Row>() {
			private static final long serialVersionUID = 3602739322755312373L;

			@Override
			public void process(Row r) {
				output.enqueue(r);
			}

			@Override
			public boolean open(long partitionId, long version) {
				return true;
			}

			@Override
			public void close(Throwable err) {}
		}).start();
		else dataset.foreach(output::enqueue);
	}

	void await() {
		if (null != streaming) try {
			streaming.awaitTermination();
		} catch (StreamingQueryException e) {
			logger.error("Stream await fail", e);
		}
	}
}
