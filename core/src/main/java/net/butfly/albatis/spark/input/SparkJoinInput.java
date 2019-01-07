package net.butfly.albatis.spark.input;

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
import scala.Tuple2;

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

	@SuppressWarnings("unchecked")
	@Override
	// 在sparkinput的构造器中调用了load()
	public List<Tuple2<String, Dataset<Row>>> load(Object context) {
		Map<String, Object> ctx = (Map<String, Object>) context;
		SparkInput<Rmap> i = (SparkInput<Rmap>) ctx.get("i");
		SparkInput<Rmap> j = (SparkInput<Rmap>) ctx.get("j");
		String ic = (String) ctx.get("ic");
		String jc = (String) ctx.get("jc");
		String t = (String) ctx.get("t");
		// 创建一个List对象,用来存放tupleList
		List<List<Tuple2<String, Dataset<Row>>>> lll = Colls.list(i.rows(), //
				new Function<Tuple2<String, Dataset<Row>>, List<Tuple2<String, Dataset<Row>>>>() {
					@Override
					public List<Tuple2<String, Dataset<Row>>> apply(Tuple2<String, Dataset<Row>> ids) {
						return Colls.list(j.rows(), new Function<Tuple2<String, Dataset<Row>>, Tuple2<String, Dataset<Row>>>() {
							@Override
							public Tuple2<String, Dataset<Row>> apply(Tuple2<String, Dataset<Row>> jds) {
								String s = ids._1 + "*" + jds._1;
								Dataset<Row> ds = ids._2.join(jds._2, ids._2.col(ic).equalTo(jds._2.col(jc)), t).distinct();
								logger().debug("Dataset joined into [" + s + "]: " + ds);
								return new Tuple2<>(s, ds);
							}
						});
					}
				});
		// 压平,后边好处理数据
		return Colls.flat(lll);
	}

	@Override
	public void close() {
		super.close();
		input.close();
		joined.close();
	}
}
