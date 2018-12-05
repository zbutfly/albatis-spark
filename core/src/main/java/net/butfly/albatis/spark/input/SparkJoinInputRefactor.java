package net.butfly.albatis.spark.input;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;
import net.butfly.albatis.spark.impl.SparkConf;
import net.butfly.albatis.spark.impl.SparkConnection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import net.butfly.albacore.io.URISpec;

public class SparkJoinInputRefactor {
	private static final long serialVersionUID = -1813416909589214047L;

//    static SparkInput<Rmap> input;

	protected final URISpec thisUri;
	protected final URISpec thatUri;
	protected final String condition;
	protected final JoinType type;
	protected final String conditionThat;



	public SparkJoinInputRefactor(URISpec thisTable, URISpec thatTable, String condition, String conditionThat, JoinType type) {
//        super(input.spark,thisTable);
        this.thisUri = thisTable;
		this.thatUri=  thatTable;
		this.condition = condition;
		this.conditionThat = conditionThat;
		this.type = type;
	}


	public  <T> List<Tuple2<String, Dataset<T>>> load() {
//	    todo 实现join的核心逻辑
//      创建dataset对象,用下边的join去实现 直接返回join的结果给test方法.
        SparkConnection conn = new SparkConnection(thisUri);
        SparkInput<Object> sub = conn.input(thatUri, TableDesc.dummy("sub"));
        SparkInput<Object> test = conn.input(thatUri, TableDesc.dummy("test"));

        List<Tuple2<String, Dataset<Row>>> rowsSub = sub.rows();
        List<Tuple2<String, Dataset<Row>>> rowsTest = test.rows();

//      todo 拿到dataset,做join处理



//      压平,后边好处理数据
//        return Colls.flat(lll);
        return null;
	}
}