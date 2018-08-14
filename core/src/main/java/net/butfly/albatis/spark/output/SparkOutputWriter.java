package net.butfly.albatis.spark.output;

import java.io.Serializable;

import org.apache.spark.sql.ForeachWriter;

import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.spark.util.DSdream;

public interface SparkOutputWriter<T> extends Serializable {
	default boolean writing(long partitionId, long version) {
		return true;
	}

	default Output<T> output() {
		return null;
	}

	void process(T t);

	class Writer<T> extends ForeachWriter<T> implements Consumer<T> {
		private static final long serialVersionUID = 3602739322755312373L;
		private final SparkOutputWriter<T> o;

		public Writer(SparkOutputWriter<T> o) {
			super();
			this.o = o;
		}

		@Override
		public void accept(T r) {
			o.process(r);
		}

		@Override
		public void process(T r) {
			if (null == r) return;
			Output<T> out = o.output();
			if (null == out) o.process(r);
			else o.output().s().statsOut(r, this);
		}

		@Override
		public boolean open(long partitionId, long version) {
			return o.writing(partitionId, version);
		}

		@Override
		public void close(Throwable err) {
			Logger.getLogger(DSdream.class).error("Writer close", err);
		}
	}
}
