package net.butfly.albatis.elastic;

import com.hzcominfo.dataggr.uniquery.Client;
import com.hzcominfo.dataggr.uniquery.dto.ResultSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.io.URISpec;
import net.butfly.alserdes.json.JsonSerDes;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * Created by 党楚翔 on 2019/6/12.
 */
public class JavaEsInputTest {

    private final static JsonSerDes json = new JsonSerDes();

    public static void main(String[] args) throws IOException {
        System.setProperty("uniquery.default.limit", "200000");
        JavaEsInputTest es = new JavaEsInputTest();
        List<Map> load = es.load();
        System.out.println(load.toString());
    }

    public List<Map> load() throws IOException {
//      TODO use esInput read es:\\  use
        URISpec uriSpec = new URISpec("es:rest://hzcominfo@172.30.10.31:39200/pengzhaung_lgxx");
        URISpec hotelURI = new URISpec("es:rest://hzcominfo@172.30.10.31:39200/hotel_info_2");
        ElasticRestHighLevelConnection conn = new ElasticRestHighLevelConnection(uriSpec);
        RestHighLevelClient restClient = conn.client;
//        RequestConfig.Builder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("pengzhaung_lgxx");
        searchRequest.types("pengzhaung_lgxx");
        searchRequest.source(searchSourceBuilder);
//        searchRequest
        SearchResponse response = restClient.search(searchRequest);
        SearchHits hits = response.getHits(); //TODO only return 10 rows,should return 10w
//        List<Map<String, Object>> allDoc = getAllDoc(hits);
//        SearchSourceBuilder builder = new SearchSourceBuilder();
//        String jsonStr = "{ \"match_all\": {}}";
//        Gson gson = new Gson();
//        JsonObject sqlJson = gson.fromJson(jsonStr, JsonObject.class);
        try {
            Client client  = new Client(new ElasticRestHighLevelConnection(uriSpec));
            Client czrkClient = new Client(new ElasticRestHighLevelConnection(hotelURI));
            long start = System.currentTimeMillis();
            ResultSet lgxxRes = client.execute("select * from pengzhaung_lgxx.pengzhaung_lgxx limit 50000"); //TODO  make select field
            System.out.println("consumer time: "+(System.currentTimeMillis() - start)/1000+"s");
            ResultSet hotel = czrkClient.execute("select * from hotel_info_2.hotel_info_2 limit 30000");
            System.out.println("consumer time: "+(System.currentTimeMillis() - start)/1000+"s");
            List<Map<String, Object>> resultLgxx = lgxxRes.getResults();   //lgxx table list //TODO foreach filter ,map iscontains key, value isEquals ,put to result
            List<Map<String, Object>> resultshotel = hotel.getResults();
//
            long startJoin = System.currentTimeMillis();
            List<Map> resultList = new ArrayList<>();
            resultLgxx.parallelStream().forEach(item -> { //10w data
                String zjhm = (String) item.get("ZJHM");//TODO equals with small table
                resultshotel.parallelStream().forEach(map -> {   //5w data
                    if (map.get("ZJHM").equals(zjhm) ) {
                        resultList.add(item); //TODO select field,should use fielddesc
                    }
                });
            } );
            System.out.println("consumer time: "+(System.currentTimeMillis() - startJoin)/1000+"s");
            client.close();
            czrkClient.close();
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
