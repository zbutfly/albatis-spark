package net.butfly.albatis.elastic;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkConnection;
import org.apache.http.HttpHost;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static net.butfly.albatis.spark.impl.Schemas.row2rmap;
import static net.butfly.albatis.spark.impl.Schemas.rmap2row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by 党楚翔 on 2019/6/14.
 */
public class GetAllDoc {
    static final Logger _logger = Logger.getLogger(GetAllDoc.class);

    URISpec uriSpec = new URISpec("es:rest://hzcominfo@172.30.10.31:39200/pengzhaung_lgxx");

    URISpec uriSpecH = new URISpec("es:rest://hzcominfo@172.30.10.31:39200/hotel_info_2");

    public static void main(String[] args) throws IOException {
        List<Map<String, Object>> allDocs = new GetAllDoc().getAllDocs();
        Dataset<Row> rowDataset = listTODS(allDocs);
//      TODO join it
        System.out.println(allDocs.size());
    }

    public List<Map<String, Object>> getAllDocs() throws IOException {
        long start = System.currentTimeMillis();

        ElasticRestHighLevelConnection conn = new ElasticRestHighLevelConnection(uriSpecH);
        RestHighLevelClient client = conn.client;
        int scrollSize = 1000;//TODO every scroll get 1000 rows
        List<Map<String, Object>> esData = new ArrayList<>();
        SearchResponse response = null;
        int i = 0;
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        while (response == null || response.getHits().getHits().length != 0) {
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.size(scrollSize);
            searchSourceBuilder.from(i * scrollSize);
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("hotel_info_2");
            searchRequest.types("hotel_info_2");
            searchRequest.source(searchSourceBuilder);
            response = client.search(searchRequest);
            response.getHits().forEach(hit -> esData.add(hit.getSourceAsMap()));
            i++;
        }
        client.close();
        _logger.info("scroll input use:"+(System.currentTimeMillis() - start)+"ms");
        return esData;
    }

    public static Dataset<Row> listTODS(List<Map<String, Object>> allDocs){
//        SparkSession spark = SparkSession.builder().master("local[2]").appName("GetAllDoc").getOrCreate(); //TODO use albatis spark
        SparkConnection sparkConnection = new SparkConnection();
        SparkSession spark = sparkConnection.spark();
        Encoder<Rmap> rowEncoder = Encoders.kryo(Rmap.class);
//      TODO should convert map to row, map to rmap ,thne make it row;
        Map<String, Object> esMap = allDocs.get(0);
        Rmap rmap = new Rmap();
        Rmap esRmap = rmap.map(esMap);

        List<Rmap> rmapList = allDocs.stream().map(v -> {
            Rmap map = new Rmap().map(v);
            return map;
        }).collect(Collectors.toList());

        Dataset<Rmap> dataset = spark.createDataset(rmapList, rowEncoder);
        Dataset<Row> rowDataset = rmap2row(TableDesc.dummy("pengzhaung_lgxx"),dataset);

//      TODO should get ds<Rmap>
//        Dataset<Row> dsString2Row = rowDataset.map(
//               value -> RowFactory.create(value, value.length())
//                , rowEncoder);
        rowDataset.show();
        rowDataset.map((MapFunction<Row, String>) item -> item.getString(0), Encoders.STRING()).show();
        return rowDataset;
    }

    Dataset<Row> join(Dataset<Row> left, String leftCol, Dataset<Row> right, String rightCol) {
        return left.distinct().join(right.distinct(), left.col(leftCol).equalTo(right.col(rightCol)), "INNER");
    }

}
