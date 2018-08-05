package net.butfly.albatis.spark.io;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

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
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.join.SparkInnerJoinInput;
import net.butfly.albatis.spark.join.SparkJoinInput;
import net.butfly.albatis.spark.join.SparkNonJoinInput;
import net.butfly.albatis.spark.join.SparkOrJoinInput;
import net.butfly.albatis.spark.plugin.PluginConfig;
import net.butfly.albatis.spark.plugin.SparkPluginInput;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class SparkConnection implements EnvironmentConnection, Serializable {
	private static final long serialVersionUID = 5093686615279489589L;
	private final static String DEFAULT_HOST = "local[*]";

	public final SparkSession spark;
	private final URISpec uriSpec;
	private final Map<String, String> parameters = Maps.of();

	public SparkConnection(String name) {
		this(name, null);
	}

	public SparkConnection(String name, URISpec uriSpec) {
		this.uriSpec = uriSpec;
		SparkConf sparkConf = new SparkConf();
		parameters.put("spark.sql.shuffle.partitions", "2001");
		// parameters.put("spark.mongodb.input.uri", "mongodb://user:pwd@localhost:80/db.tbl");
		// parameters.put("spark.mongodb.output.uri", "mongodb://user:pwd@localhost:80/db.tbl");
		parameters.forEach((key, value) -> sparkConf.set(key, value));
		String host = uriSpec.getHost();
		if (host.isEmpty()) host = DEFAULT_HOST;
		this.spark = SparkSession.builder().master(host).appName(name == null ? "Simulation" : name).config(sparkConf).getOrCreate();
	}

	@Override
	public void close() {
		if (spark != null) {
			spark.close();
		}
	}

	@Override
	public <M extends Rmap> Input<M> input(String... table) throws IOException {
		if (table.length == 0) throw new IllegalArgumentException("Spark connection open input with first argument as target db uri");
		String[] ts = Colls.list(table).subList(1, table.length - 1).toArray(new String[0]);
		return input(new URISpec(table[0]), ts);
	}

	@Override
	@Deprecated
	public <M extends Rmap> Output<M> output() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public <V, O extends Output<V>> O output(URISpec uri) {
		return SparkIO.output(spark, uri);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V, I extends Input<V>> I input(URISpec uri, String... table) {
		return (I) SparkIO.input(spark, uri, table);
	}

	public <V> SparkJoinInput innerJoin(SparkInputBase<Row> input, String col, Map<SparkInputBase<?>, String> joinInputs) {
		return new SparkInnerJoinInput(input, col, joinInputs);
	}

	public SparkJoinInput orJoin(SparkInputBase<Row> input, String col, Map<SparkInputBase<?>, String> joinInputs) {
		return new SparkOrJoinInput(input, col, joinInputs);
	}

	public SparkJoinInput nonJoin(SparkInputBase<Row> input, String col, Map<SparkInputBase<?>, String> joinInputs) {
		return new SparkNonJoinInput(input, col, joinInputs);
	}

	@SuppressWarnings("unchecked")
	public SparkPluginInput plugin(String className, SparkInputBase<Rmap> input, PluginConfig pc) {
		try {
			Class<? extends SparkPluginInput> c = (Class<? extends SparkPluginInput>) Class.forName(className);
			return c.getConstructor(SparkInputBase.class, PluginConfig.class).newInstance(input, pc);
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
			return new SparkConnection("SparkConnection", uriSpec);
		}

		@Override
		public List<String> schemas() {
			return Colls.list("spark");
		}
	}
}
