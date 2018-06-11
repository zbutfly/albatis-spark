package com.hzcominfo.dataggr.uniquery.mongo.test;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.SqlExplainer;
import com.hzcominfo.dataggr.uniquery.mongo.MongoQuery;
import com.hzcominfo.dataggr.uniquery.mongo.MongoQueryVisitor;
import com.mongodb.*;
import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.mongodb.MongoConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class MongoQueryTest {
    private static final String uri = "mongodb://yjdb:yjdb1234@172.30.10.101:22001/yjdb/YJDB_GAZHK_WBXT_SWRY_XXB?readPreference=primary";
    private MongoConnection connection;
    private DBCollection collection;

    @Before
    public void b() throws IOException {
        connection = new MongoConnection(new URISpec(uri));
        collection = connection.collection();
    }

    @After
    public void a() {
        if (null != connection) connection.close();
    }

    @Test
    public void t1() {
        System.out.println(collection.getFullName() + " count: " + collection.count());
        // like
        String like = "智力%";
        if (!like.startsWith("%")) like = "^" + like;
        if (!like.endsWith("%")) like = like + "$";
//        like = like.replaceAll("_", ".").replaceAll("%", ".*");
        like = like.replaceAll("_", ".").replaceAll("%", ".*");
        Pattern pattern = Pattern.compile(like, Pattern.CASE_INSENSITIVE);
        QueryBuilder builder = QueryBuilder.start("SERVICE_NAME").regex(pattern);
        DBObject query = builder.get();
        System.out.println("query:" + query);
        DBObject fields = new BasicDBObject();

        fields.put("SERVICE_CODE", 1);
        fields.put("USER_NAME", 1);
        DBCursor cursor = collection.find(query, fields);
//        cursor.skip(10);
//        cursor.limit(20);

        AtomicInteger idx = new AtomicInteger(0);
        cursor.forEach(o -> System.out.println(idx.incrementAndGet() + " --->  " + o));
        connection.close();
//        System.out.println(builder.get());
    }

    /* fields test */
    @Test
    public void f1() {
        /*unrecognized*/
//        String sql = "select * from YJDB_GAZHK_WBXT_SWRY_XXB";
        /*recognized*/
        String sql = "select _id, CERTIFICATE_CODE, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB";
        /*impure*/
//        String sql = "select *, _id, CERTIFICATE_CODE, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB";

        JsonObject json = SqlExplainer.explain(sql);
        MongoQuery mq = new MongoQueryVisitor(new MongoQuery(), json).get();
        DBObject query = mq.getQuery();
        DBObject fields = mq.getFields();
        DBCursor cursor = collection.find(query, fields);
        AtomicInteger idx = new AtomicInteger();
        cursor.iterator().forEachRemaining(o -> System.out.println(idx.incrementAndGet() + "--->" + o));
    }

    /* condition test */
    @Test
    public void w1() {
        /*unrecognized*/
//        String sql = "select _id, CERTIFICATE_CODE, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB";
        /*equals*/
//        String sql = "select SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME = '智力网吧'";
        /*not equals*/
//        String sql = "select SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME != '智力网吧'";
        /*less than*/
//        String sql = "select SERVICE_NAME, SERVICE_CODE from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_CODE < '3301060145'";
        /*less than or equal*/
//        String sql = "select SERVICE_NAME, SERVICE_CODE from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_CODE <= '3301060145'";
        /*great than*/
//        String sql = "select SERVICE_NAME, SERVICE_CODE from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_CODE > '3301060145'";
        /*great or equal*/
//        String sql = "select SERVICE_NAME, SERVICE_CODE from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_CODE >= '3301060145'";
        /*is null*/
//        String sql = "select _id, SERVICE_CODE, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where OFFLINE_TIME IS NULL";
        /*is not null*/
//        String sql = "select _id, SERVICE_CODE, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where OFFLINE_TIME IS not NULL";
        /*like*/
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME LIKE '%网咖'";
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME LIKE '新%'";
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME LIKE '新%网%'";
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME LIKE '新_网%'";
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME LIKE '新__网%'";
        /*not like*/
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME not LIKE '%网咖'";
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME not LIKE '新%'";
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME not LIKE '新%网%'";
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME not LIKE '新_网%'";
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME not LIKE '新__网%'";
        /*between*/
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_CODE between '3301040007' and '3301040019'";
        /*not between*/
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_CODE not between '3301040007' and '3301040019'";
        /*in*/
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_CODE in ('3301040007', '3301040019')";
        /*not in*/
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_CODE not in ('3301040007', '3301040019')";
        /*and*/
//        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME = '新丰网吧' and OFFLINE_TIME is null";
        /*or*/
        String sql = "select SERVICE_CODE, SERVICE_NAME, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_NAME = '丁丁网吧' or SERVICE_NAME = '仁和网吧'";

        JsonObject json = SqlExplainer.explain(sql);
        MongoQuery mq = new MongoQueryVisitor(new MongoQuery(), json).get();
        DBObject query = mq.getQuery();
        DBObject fields = mq.getFields();
        System.out.println("query: " + query);
        DBCursor cursor = collection.find(query, fields);
        AtomicInteger idx = new AtomicInteger();
        cursor.iterator().forEachRemaining(o -> System.out.println(idx.incrementAndGet() + "--->" + o));
    }

    /*sort test*/
    @Test
    public void s1() {
//        String sql = "select SERVICE_CODE, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB";
//        String sql = "select SERVICE_CODE, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB order by SERVICE_CODE";
//        String sql = "select SERVICE_CODE, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB order by SERVICE_CODE asc";
//        String sql = "select SERVICE_CODE, USER_NAME from YJDB_GAZHK_WBXT_SWRY_XXB order by SERVICE_CODE desc";
//        String sql = "select SERVICE_CODE, COMPUTER_NO from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_CODE in ('3301040007', '3301040019') order by SERVICE_CODE desc, COMPUTER_NO DESC";
        String sql = "select SERVICE_CODE, COMPUTER_NO from YJDB_GAZHK_WBXT_SWRY_XXB where SERVICE_CODE in ('3301040007', '3301040019') order by SERVICE_CODE desc, COMPUTER_NO";
        JsonObject json = SqlExplainer.explain(sql);
        MongoQuery mq = new MongoQueryVisitor(new MongoQuery(), json).get();
        DBObject query = mq.getQuery();
        DBObject fields = mq.getFields();
        DBCursor cursor = collection.find(query, fields).sort(mq.getSort());
        AtomicInteger idx = new AtomicInteger();
        cursor.iterator().forEachRemaining(o -> System.out.println(idx.incrementAndGet() + "---->" + o));
    }

    /*offset and limit test*/
    @Test
    public void o1() {
//        String sql = "select SERVICE_CODE, COMPUTER_NO from YJDB_GAZHK_WBXT_SWRY_XXB order by SERVICE_CODE";
        String sql = "select SERVICE_CODE, COMPUTER_NO from YJDB_GAZHK_WBXT_SWRY_XXB order by SERVICE_CODE limit 10";
//        String sql = "select SERVICE_CODE, COMPUTER_NO from YJDB_GAZHK_WBXT_SWRY_XXB order by SERVICE_CODE limit 10 offset 10 ";
        JsonObject json = SqlExplainer.explain(sql);
        MongoQuery mq = new MongoQueryVisitor(new MongoQuery(), json).get();
        DBObject query = mq.getQuery();
        DBObject fields = mq.getFields();
        DBCursor cursor = collection.find(query, fields).sort(mq.getSort()).skip(mq.getOffset()).limit(mq.getLimit());
        AtomicInteger idx = new AtomicInteger();
        cursor.iterator().forEachRemaining(o -> System.out.println(idx.incrementAndGet() + "---->" + o));
    }

}
