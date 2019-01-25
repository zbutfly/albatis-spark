package net.butfly.albatis.spark.input;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.SparkRowInput;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.*;

import static org.apache.spark.sql.functions.lit;

public class SparkJoinInput extends SparkRowInput {
	private static final long serialVersionUID = -4870210186801499L;
	public final SparkInput<Rmap> input;
	public final String col;
	public final SparkInput<Rmap> joined;
	public final String joinedCol;
	public final String joinType;

	public SparkJoinInput(SparkInput<Rmap> input, String col, SparkInput<Rmap> joined, String joinedCol, String joinType, String finallyJoinName, Set<String> leftSet, Set<String> rightSet, String taskId) {
		super(input.spark, input.targetUri, Maps.of("leftInput", input, "rightInput", joined, "leftCol", col, "rightCol", joinedCol, "type", joinType, "as", finallyJoinName,"leftSet",leftSet,"rightSet",rightSet,"taskId",taskId));
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
	public List<Tuple2<String, Dataset<Row>>> load(Object context) {
		Map<String, Object> ctx = (Map<String, Object>) context;

		String taskId = (String) ctx.get("taskId");
		String leftCol = (String) ctx.get("leftCol");
		String rightCol = (String) ctx.get("rightCol");

		SparkInput<Rmap> leftInput = (SparkInput<Rmap>) ctx.get("leftInput");
		String leftTableName = leftInput.rows().get(0)._1;

		Dataset<Row> leftDS = null;
		if (null != ctx.get("leftSet")){
			Set<String> leftSet = (Set<String>) ctx.get("leftSet");
			leftDS = getPurgedDS(leftInput, leftSet, leftCol);
		}else {
//			joinInput的处理逻辑
			leftDS = leftInput.rows().get(0)._2;
//			leftDS.show();
		}


		List<Tuple2<String, Dataset<Row>>> leftRows = Colls.list(new Tuple2<>(leftTableName, leftDS));

        SparkInput<Rmap> rightInput = (SparkInput<Rmap>) ctx.get("rightInput");
		String rightTableName = rightInput.rows().get(0)._1;

		Dataset<Row> rightDS = null;
//		todo 测试完drop cancel注释
		if (null != ctx.get("rightSet")){
			Set<String> rightSet = (Set<String>) ctx.get("rightSet");
			rightDS = getPurgedDS(rightInput, rightSet, rightCol);
		}else{
//			joinInput处理逻辑
			rightDS = rightInput.rows().get(0)._2;
//			rightDS.show();
		}


		List<Tuple2<String, Dataset<Row>>> rightRows = Colls.list(new Tuple2<>(rightTableName, rightDS));

		List<List<Tuple2<String, Dataset<Row>>>> lll = Colls.list(leftRows, left -> Colls.list(rightRows, //
				right -> doJoin(left, right, (String) ctx.get("leftCol"), (String) ctx.get("rightCol"), //
						(String) ctx.get("type"), (String) ctx.get("as"),leftTableName,rightTableName,taskId)));
		return Colls.flat(lll);
	}

	public Tuple2<String, Dataset<Row>> doJoin(Tuple2<String, Dataset<Row>> ids, Tuple2<String, Dataset<Row>> jds, String ic, String jc,
											   String type, String asTable, String leftName, String rightName, String taskId){
		Dataset<Row> main = ids._2;
		String[] columnsMain = main.columns();

		Dataset<Row> sub = jds._2;
		String[] columnsSub = sub.columns();

		String joinName = asTable;//判断最后一次join

		Dataset<Row> ds = main.join(sub, main.col(ic).equalTo(sub.col(jc)), type).distinct();

		Dataset<Row> selectDS = null; //构造别名
		if (null != leftName || null != rightName){
			List<Column> list = new ArrayList<>();
			if (null != leftName && null != rightName){ //如果leftName是空,就执行right的加别名逻辑; 如果都不空,就都执行;
				for(String colStr : columnsMain){
					list.add(main.col(colStr).as(addPrefix(leftName, colStr)));
				}
				for(String colStr : columnsSub){
					list.add(sub.col(colStr).as(addPrefix(rightName, colStr)));
				}
			}else{  //进来的是普通input和joinInput的数据;
				if (null == leftName){
					oddDSPrefix(rightName, main, columnsMain, sub, columnsSub, list);
				}
				if (null == rightName){
					oddDSPrefix(leftName, sub, columnsSub, main, columnsMain, list);
				}
			}
			selectDS = ds.select(convertListToSeq(list));
		}else{
			selectDS = ds;  //中间数据碰撞不加别名
//			selectDS.show(1);
		}

		if (null != asTable){
			assert selectDS != null;
			Dataset<Row> resultDS = selectDS.withColumn("TASKID", lit(taskId));
			resultDS.show(1);
			return new Tuple2<>(joinName, resultDS);
		}
		return new Tuple2<>(joinName, ds);
	}

	private void oddDSPrefix(String rightName, Dataset<Row> main, String[] columnsMain, Dataset<Row> sub, String[] columnsSub, List<Column> list) {
		for(String colStr : columnsSub){
			list.add(sub.col(colStr).as(addPrefix(rightName, colStr)));//给右表加prefix
		}
		for(String colStr : columnsMain){
			list.add(main.col(colStr)); //左表不加
		}
	}


	private Dataset<Row> getPurgedDS(SparkInput<Rmap> input, Set<String> fields, String joincol) {
			Dataset<Row> ds = input.rows().get(0)._2;
			Set<String> addFields = new HashSet<>();
			String[] columns = ds.columns();
			for (String col : columns){
				addFields.add(col);
			}
//			不drop碰撞字段   如果碰撞字段是重复的,就drop
			addFields.remove(joincol);
			// 存放非展示字段
			Set<String> resultSet = new HashSet<>();
			resultSet.clear();
			resultSet.addAll(addFields);
			resultSet.removeAll(fields);
			Dataset<Row> purgeDS = purge(ds, resultSet);
			return purgeDS;
	}

	private Dataset<Row> purge(Dataset<Row> ds, Set<String> fields_list) {
		Dataset<Row> d = ds;
		for (String f : fields_list)
			if (ds.schema().getFieldIndex(f).nonEmpty()) d = d.drop(ds.col(f));
		return d;
	}


	private String addPrefix(String tableName,String oldColumn) {
		String resultName = tableName + "_" + oldColumn;
		return resultName;
	}

	public Seq<Column> convertListToSeq(List<Column> inputList) {
		return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}

	@Override
	public void close() {
		super.close();
		input.close();
		joined.close();
	}
}
