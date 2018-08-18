package net.butfly.albatis.spark.impl;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.albatis.nosql.EnvironmentConnection;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.IOs;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.input.SparkInnerJoinInput;
import net.butfly.albatis.spark.input.SparkJoinInput;
import net.butfly.albatis.spark.input.SparkNonJoinInput;
import net.butfly.albatis.spark.input.SparkOrJoinInput;
import net.butfly.albatis.spark.plugin.PluginConfig;
import net.butfly.albatis.spark.plugin.SparkPluginInput;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class SparkConnection implements EnvironmentConnection {
	private static final long serialVersionUID = 5093686615279489589L;
	private final static String DEFAULT_HOST = "local[*]";

	public final SparkSession spark;
	private final URISpec uriSpec;

	public SparkConnection() {
		this(null);
	}

	public SparkConnection(URISpec uriSpec) {
		this.uriSpec = uriSpec;
		SparkConf sparkConf = new SparkConf();
		Map<String, String> params = params();
		String host = host(params);
		params().forEach(sparkConf::set);
		if (host.isEmpty()) host = DEFAULT_HOST;
		String name = Systems.getMainClass().getSimpleName();
		logger.info("Spark [" + name + "] constructing with config: \n" + sparkConf.toDebugString());
		this.spark = SparkSession.builder().master(host).appName(name).config(sparkConf).getOrCreate();
	}

	private String host(Map<String, String> params) {
		String host = params.remove("spark.host");
		String paral = params.remove("spark.parallelism");
		if (null == paral) paral = "*";
		if (null != host) host = host + "[" + paral + "]";
		else host = uriSpec.getHost();
		if (null == host || host.isEmpty()) host = DEFAULT_HOST;
		logger.debug("Spark host detected: " + host);
		return host;
	}

	private Map<String, String> params() {
		Properties props = new Properties();
		try {
			props.load(IOs.open("spark.properties"));
		} catch (IOException e) {
			logger.warn("Default spark configuration file [classpath:spark.properties] loading failed.");
		}

		Map<String, String> params = Maps.of();
		props.forEach((k, v) -> {
			String vv = v.toString().trim();
			if (!vv.isEmpty()) params.put(k.toString().trim(), vv);
		});
		// fxxking mongodb-spark-connector
		params.putIfAbsent("spark.mongodb.input.uri", "mongodb://127.0.0.1/FxxkMongoSpark.FakeCollection");
		params.putIfAbsent("spark.mongodb.output.uri", "mongodb://127.0.0.1/FxxkMongoSpark.FakeCollection");

		return params;
	}

	@Override
	public void close() {
		if (spark != null) {
			spark.close();
		}
	}

	@Override
	public <M extends Rmap> Input<M> input(String... table) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	@Deprecated
	public <M extends Rmap> Output<M> output(String... table) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <V, O extends Output<V>> O output(URISpec uri, String... table) {
		return SparkIO.output(spark, uri, table);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V, I extends Input<V>> I input(URISpec uri, String... table) {
		return (I) SparkIO.input(spark, uri, table);
	}

	public <V> SparkJoinInput innerJoin(SparkInput<Row> input, String col, Map<SparkInput<?>, String> joinInputs) {
		return new SparkInnerJoinInput(input, col, joinInputs);
	}

	public SparkJoinInput orJoin(SparkInput<Row> input, String col, Map<SparkInput<?>, String> joinInputs) {
		return new SparkOrJoinInput(input, col, joinInputs);
	}

	public SparkJoinInput nonJoin(SparkInput<Row> input, String col, Map<SparkInput<?>, String> joinInputs) {
		return new SparkNonJoinInput(input, col, joinInputs);
	}

	@SuppressWarnings("unchecked")
	public SparkPluginInput plugin(String className, SparkInput<Rmap> input, PluginConfig pc) {
		try {
			Class<? extends SparkPluginInput> c = (Class<? extends SparkPluginInput>) Class.forName(className);
			return c.getConstructor(SparkInput.class, PluginConfig.class).newInstance(input, pc);
		} catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
				| IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e.getTargetException());
		}
	}

	@Override
	public String defaultSchema() {
		return "spark";
	}

	@Override
	public URISpec uri() {
		return uriSpec;
	}

	public <T> Dataset<T> toDS(List<T> rows, Class<T> clazz) {
		return spark.createDataset(rows, Encoders.bean(clazz));
	}

	/**
	 * job start stage <br/>
	 * stage submit tasks <br/>
	 * task end index <br/>
	 * stage completed <br/>
	 * job end
	 */
	public void addSparkListener() {
		spark.sparkContext().addSparkListener(new SparkListener() {

			@Override
			public void onJobStart(SparkListenerJobStart jobStart) {
				Seq<Object> seq = jobStart.stageIds();
				List<Object> stageIds = JavaConverters.seqAsJavaListConverter(seq).asJava();
				SparkSchedule.addStages(jobStart.jobId(), stageIds);
			}

			@Override
			public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
				SparkSchedule.addTaskNums(stageSubmitted.stageInfo().stageId(), stageSubmitted.stageInfo().numTasks());
			}

			@Override
			public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
				SparkSchedule.complete(stageCompleted.stageInfo().stageId());
			}

			@Override
			public void onTaskStart(SparkListenerTaskStart taskStart) {
				SparkSchedule.addTasks(taskStart.stageId(), taskStart.taskInfo().index());
			}

			@Override
			public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
				SparkSchedule.addTasks(taskEnd.stageId(), taskEnd.taskInfo().index());
			}
		});
	}

	public int getJobId() {
		return spark.sparkContext().dagScheduler().nextJobId().get();
	}

	public void endJob(String key) {
		Integer jobId = SparkSchedule.getJob(key);
		if (jobId == null) return;
		spark.sparkContext().cancelJob(jobId);
	}

	public Dataset<Row> sql(String sql) {
		return spark.sql(sql);
	}

	public static class Driver implements com.hzcominfo.albatis.nosql.Connection.Driver<SparkConnection> {
		static {
			DriverManager.register(new Driver());
		}

		@Override
		public SparkConnection connect(URISpec uriSpec) throws IOException {
			return new SparkConnection(uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("spark");
		}
	}
}
