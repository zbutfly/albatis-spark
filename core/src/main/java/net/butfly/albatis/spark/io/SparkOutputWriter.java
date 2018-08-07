package net.butfly.albatis.spark.io;

import java.io.Serializable;

import org.apache.spark.sql.ForeachWriter;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.spark.util.DSdream;

public interface SparkOutputWriter<T> extends Serializable {
	default boolean writing(long partitionId, long version) {
		return true;
	}

	void process(T t);

	default ForeachWriter<T> w() {
		return new ForeachWriter<T>() {
			private static final long serialVersionUID = 3602739322755312373L;

			@Override
			public void process(T r) {
				if (null != r) SparkOutputWriter.this.process(r);
			}

			@Override
			public boolean open(long partitionId, long version) {
				return writing(partitionId, version);
			}

			@Override
			public void close(Throwable err) {
				Logger.getLogger(DSdream.class).error("Writer close", err);
			}
		};
	}

	default String format() {
		return null;
	}
}
