//package net.butfly.albatis.elastic;
//
//import net.butfly.albacore.io.URISpec;
//import net.butfly.albacore.utils.logger.Logger;
//import net.butfly.alserdes.json.JsonSerDes;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.Encoder;
//import org.apache.spark.sql.Encoders;
//import static net.butfly.albatis.spark.impl.Schemas.row2rmap;
//
//
//
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//
//
///**
// * Created by 党楚翔 on 2019/6/12.
// */
//public class JavaCollectTODataset {
//
//    private final static JsonSerDes json = new JsonSerDes();
//    private SparkSession spark = null;
//
//    static  final Logger logger = Logger.getLogger(JavaCollectTODataset.class);
//
//    public static void main(String[] args) throws IOException {
//        JavaCollectTODataset es = new JavaCollectTODataset();
//        List<Map> load = es.load();
//    }
//
//    public List<Map> load() throws IOException {
//        URISpec hotelUri = new URISpec("es:rest://hzcominfo@172.30.10.31:39200/hotel_info_2");
//        try {
//            long startJoin = System.currentTimeMillis();
////          TODO  create sc, use parallize conversion to dataset
//            Encoder<Row> rowEncoder = Encoders.javaSerialization(Row.class);
//            Dataset<Row> dsString2Row = ds.map(
//                    ( value -> RowFactory.create(value, value.length())
//                    , rowEncoder);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//
//    Dataset<Row> join(Dataset<Row> left, String leftCol, Dataset<Row> right, String rightCol) {
//        return left.distinct().join(right.distinct(), left.col(leftCol).equalTo(right.col(rightCol)), type);
//    }
//}
