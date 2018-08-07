package net.butfly.albatis.spark.io;

import java.io.Serializable;

import org.apache.spark.sql.ForeachWriter;

import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.util.DSdream;

public interface SparkOutputWriter<T> extends Serializable {
	default boolean writing(long partitionId, long version) {
		return true;
	}

	void process(T t);

	default String format() {
		return null;
	}

	class Writer<T> extends ForeachWriter<T> {
		private static final long serialVersionUID = 3602739322755312373L;
		private final SparkOutput o;

		public Writer(SparkOutput o) {
			super();
			this.o = o;
		}

		private void using(T r) {
			o.process((Rmap) r);
		}

		@Override
		public void process(T r) {
			if (null != r) o.s().statsOut(r, this::using);
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
