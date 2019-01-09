package net.butfly.albatis.spark.input;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.SparkRowInput;
import scala.*;
import scala.collection.*;
import scala.collection.mutable.ArraySeq;
import scala.collection.mutable.Seq;

public class SparkJoinInput extends SparkRowInput {
	private static final long serialVersionUID = -4870210186801499L;
	public final SparkInput<Rmap> input;
	public final String col;
	public final SparkInput<Rmap> joined;
	public final String joinedCol;
	public final String joinType;

	public SparkJoinInput(SparkInput<Rmap> input, String col, SparkInput<Rmap> joined, String joinedCol, String joinType) {
		super(input.spark, input.targetUri, Maps.of("i", input, "j", joined, "ic", col, "jc", joinedCol, "t", joinType));
		this.input = input;
		this.col = col;
		this.joined = joined;
		this.joinedCol = joinedCol;
		this.joinType = joinType;
	}

	@Override
	public void open() {
		joined.open();
		input.open();
		super.open();
	}


	public scala.collection.Seq<String> convertListToSeq(List<String> inputList) {
		return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Tuple2<String, Dataset<Row>>> load(Object context) {
		Map<String, Object> ctx = (Map<String, Object>) context;
		SparkInput<Rmap> i = (SparkInput<Rmap>) ctx.get("i");
		SparkInput<Rmap> j = (SparkInput<Rmap>) ctx.get("j");
		String ic = (String) ctx.get("ic");
		String jc = (String) ctx.get("jc");
		String type = (String) ctx.get("t");
		List<List<Tuple2<String, Dataset<Row>>>> lll = Colls.list(i.rows(), //
				new Function<Tuple2<String, Dataset<Row>>, List<Tuple2<String, Dataset<Row>>>>() {
					@Override
					public List<Tuple2<String, Dataset<Row>>> apply(Tuple2<String, Dataset<Row>> ids) {
						return Colls.list(j.rows(), new Function<Tuple2<String, Dataset<Row>>, Tuple2<String, Dataset<Row>>>() {
							@Override
							public Tuple2<String, Dataset<Row>> apply(Tuple2<String, Dataset<Row>> jds) {
								Dataset<Row> main = ids._2;
								Dataset<Row> sub = jds._2;
								String joinName = main + "*" + sub;
								Dataset<Row> ds = main.join(sub, main.col(ic).equalTo(sub.col(jc)), type).distinct();
//								ds.show(1);
//								todo drop副表的字段和条件 动态处理
								Dataset<Row> drop1 = ds.drop(sub.col("___table")).drop(sub.col("___key_value")).drop(sub.col("_id"))
										.drop(sub.col("GMSFHM_s")).drop(sub.col("HYZT")).drop(sub.col("XB")).drop(sub.col("NAME"));
								drop1.show(1);
//								logger().debug("Dataset joined into [" + s + "]: " + ds);
								return new Tuple2<>(joinName, drop1);
							}
						});
					}
				});
		return Colls.flat(lll);
	}

	@Override
	public void close() {
		super.close();
		input.close();
		joined.close();
	}
}
