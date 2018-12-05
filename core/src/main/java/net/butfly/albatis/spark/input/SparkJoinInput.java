package net.butfly.albatis.spark.input;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import scala.Tuple2;

public class SparkJoinInput extends SparkInput<Rmap> {
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
//  在sparkinput的构造器中调用了load()
	public List<Tuple2<String, Dataset<Row>>> load() {
//		创建一个List对象,用来存放tupleList
		List<List<Tuple2<String, Dataset<Row>>>> lll = Colls.list(input.rows(),
//				//传入一个迭代器,一个function
//				创建t1,t2.通过DataSet的join方法,做一个碰撞, col就是t1的条件字段,joinedCol就是t2的条件字段. 使用distinct拿到join后新的的Dataset
				table1 -> Colls.list(joined.rows(), table2 -> new Tuple2<>(table1._1 + "*" + table2._1,
						table1._2.join(table2._2, table1._2.col(col).equalTo(table2._2.col(joinedCol)), joinType).distinct())));
//      压平,后边好处理数据
        return Colls.flat(lll);
	}




	@Override
	public void close() {
		super.close();
		input.close();
		joined.close();
	}
}
