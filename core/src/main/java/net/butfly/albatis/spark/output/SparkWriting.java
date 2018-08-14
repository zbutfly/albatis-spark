package net.butfly.albatis.spark.output;

public interface SparkWriting {
	boolean writing(long partitionId, long version);

	default void close(Throwable err) {}
}
