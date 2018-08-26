package net.butfly.albatis.spark.input;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import scala.Tuple2;

public class SparkJoinInput extends SparkInput<Rmap> {
	private static final long serialVersionUID = -1813416909589214047L;
	protected final SparkInput<Rmap> input;
	protected final String col;
	protected final SparkInput<Rmap> joined;
	protected final String joinedCol;
	protected final String joinType;

	public SparkJoinInput(SparkInput<Rmap> input, String col, SparkInput<Rmap> joined, String joinedCol, String joinType) {
		super(input.spark, input.targetUri);
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
	protected List<Tuple2<String, Dataset<Row>>> load() {
		List<List<Tuple2<String, Dataset<Row>>>> lll = Colls.list(input.rows(), //
				t -> Colls.list(joined.rows(), t1 -> new Tuple2<>(t._1 + "*" + t1._1, //
						t._2.join(t1._2, t._2.col(col).equalTo(t1._2.col(joinedCol)), joinType).distinct())));
		return Colls.flat(lll);
	}

	@Override
	public void close() {
		super.close();
		input.close();
		joined.close();
	}
}
