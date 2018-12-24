package com.hzcominfo.search.collision.test;

import com.google.gson.Gson;
import com.hzcominfo.search.collision.InitThread;
import com.hzcominfo.search.collision.InitThreadImpl;
import com.hzcominfo.search.collision.dto.CollisionResponse;
import com.hzcominfo.search.collision.dto.Collisionquest;
import com.hzcominfo.search.collision.impl.facade.CollisionFacadeImpl;
import com.hzcominfo.search.collision.mapper.CollisionReq;
import com.hzcominfo.search.collision.util.JsonUtils;
import net.butfly.albacore.exception.BusinessException;

/**
 * Created by 党楚翔 on 2018/12/12.
 */
public class facadeTest
{
    public static void main(String[] args) {
        CollisionFacadeImpl collisionFacade = new CollisionFacadeImpl();
//      拼接一个jsonObj,用来做调试;
        Collisionquest quest = new Collisionquest();
        quest.setUserName("dcx");
        quest.setPkiName("pkiName");
//      这还要setkey,因为那边id取的就是getKy
        quest.setTaskKey("1224test7");
        quest.setCornInfo("*/30 * * *");
        quest.setType("1");
//        quest.setNodes("[{\"userName\":\"panyc\",\"taskId\":\"testKey\",\"cornInfo\":\"\",\"type\":\"1\",\"nodes\":[{\"type\":\"0\",\"nodes\":[{\"tableName\":\"ORACLE_MONGO_CZRK\",\"tableDisplayName\":\"常住人口\",\"tableConnect\":\"zk://data01:7181,data02:7181,data03:7181\",\"queryParam\":\"limit 100\",\"idDefineName\":\"GMSFHM_s\",\"fieldSet\":{\"GMSFHM_s\":\"证件号码\",\"HJDZ_XZ_s\":\"现住地址\"},\"selectSize\":\"200\"}]},{\"type\":\"0\",\"nodes\":[{\"tableName\":\"ORACLE_MONGO_CZRK\",\"tableDisplayName\":\"常住人口\",\"tableConnect\":\"es://hzcominfo@172.30.10.31:39300/dpc_test?httpport=39200\",\"queryParam\":\"limit 100\",\"idDefineName\":\"GMSFHM_s\",\"fieldSet\":{\"GMSFHM_s\":\"证件号码\",\"HJDZ_XZ_s\":\"现住地址\"},\"selectSize\":\"200\"}]}]}]");
//        quest.setConditions("{\"mainFlag\":\"0\",\"tableName\":\"ORACLE_MONGO_CZRK\",\"tableDisplayName\":\"常住人口\",\"tableConnect\":\"zk\",\"queryParam\":\"limit 100\",\"idDefineName\":\"GMSFHM_s\",\"fieldSet\":{\"GMSFHM_s\":\"证件号码\",\"HJDZ_XZ_s\":\"现住地址\"},\"selectSize\":\"200\"},{\"mainFlag\":\"1\",\"tableName\":\"ES_ORACLE_MONGO_CZRK\",\"tableDisplayName\":\"ES常住人口\",\"tableConnect\":\"es\",\"queryParam\":\"where GMSFHM_s like '30%' \",\"idDefineName\":\"GMSFHM_s\",\"fieldSet\":{\"GMSFHM_s\":\"证件号码\",\"JG_SSXQ_FORMAT_s\":\"现住地址\"},\"selectSize\":\"200\"}");
//        quest.setInfo("{\"type\":\"1\",\"data\":[{\"mainFlag\":\"0\",\"tableName\":\"ORACLE_MONGO_CZRK\",\"tableDisplayName\":\"常住人口\",\"tableConnect\":\"zk\",\"queryParam\":\"limit 100\",\"idDefineName\":\"GMSFHM_s\",\"fieldSet\":{\"GMSFHM_s\":\"证件号码\",\"HJDZ_XZ_s\":\"现住地址\"},\"selectSize\":\"200\"},{\"mainFlag\":\"1\",\"tableName\":\"ES_ORACLE_MONGO_CZRK\",\"tableDisplayName\":\"ES常住人口\",\"tableConnect\":\"es\",\"queryParam\":\"where GMSFHM_s like '30%' \",\"idDefineName\":\"GMSFHM_s\",\"fieldSet\":{\"GMSFHM_s\":\"证件号码\",\"JG_SSXQ_FORMAT_s\":\"现住地址\"},\"selectSize\":\"200\"}],\"parents\":[{\"type\":\"1\",\"data\":[{}],\"parents\":[{}]},{\"type\":\"1\",\"data\":[],\"parents\":[{}]}]}");
//      在facade里,应该是setnodes的json
//        quest.setNodes("{\"nodes\":[{\"type\":\"0\",\"nodes\":[{\"tableName\":\"ORACLE_MONGO_CZRK\",\"tableDisplayName\":\"常住人口\",\"tableConnect\":\"zk://data01:7181,data02:7181,data03:7181\",\"queryParam\":\"limit 100\",\"idDefineName\":\"GMSFHM_s\",\"fieldSet\":{\"GMSFHM_s\":\"证件号码\",\"HJDZ_XZ_s\":\"现住地址\"},\"selectSize\":\"200\"}]},{\"type\":\"0\",\"nodes\":[{\"tableName\":\"ORACLE_MONGO_CZRK\",\"tableDisplayName\":\"常住人口\",\"tableConnect\":\"zk://data01:7181,data02:7181,data03:7181\",\"queryParam\":\"limit 100\",\"idDefineName\":\"GMSFHM_s\",\"fieldSet\":{\"GMSFHM_s\":\"证件号码\",\"HJDZ_XZ_s\":\"现住地址\"},\"selectSize\":\"200\"}]}]}");
//        quest.setNodes("{\"nodes\":[{\"type\":\"0\",\"nodes\":[{\"tableName\":\"ORACLE_MONGO_CZRK\",\"tableDisplayName\":\"常住人口\",\"tableConnect\":\"zk://data01:7181,data02:7181,data03:7181\",\"queryParam\":\"limit 100\",\"idDefineName\":\"GMSFHM_s\",\"fieldSet\":{\"GMSFHM_s\":\"证件号码\",\"HJDZ_XZ_s\":\"现住地址\"},\"selectSize\":\"200\"}]},{\"type\":\"0\",\"nodes\":[{\"tableName\":\"ES_ORACLE_MONGO_CZRK\",\"tableDisplayName\":\"ES常住人口\",\"tableConnect\":\"es://hzcominfo@172.30.10.31:39300/dpc_test?httpport=39200\",\"queryParam\":\"limit 100\",\"idDefineName\":\"GMSFHM_s\",\"fieldSet\":{\"GMSFHM_s\":\"证件号码\",\"JG_SSXQ_FORMAT_s\":\"现住地址\"},\"selectSize\":\"200\"}]}]}");

//     二条数据测试
//        quest.setNodes("{\n" +
//                "    \"nodes\": [\n" +
//                "        {\n" +
//                "            \"type\": \"1\",\n" +
//                "            \"nodes\": [\n" +
//                "                {\n" +
//                "                    \"type\": \"0\",\n" +
//                "                    \"nodes\": [\n" +
//                "                        {\n" +
//                "                            \"idDefineName\": \"GMSFHM_s\",\n" +
//                "                            \"selectSize\": \"200\",\n" +
//                "                            \"fieldSet\": {\n" +
//                "                                \"XM_s\": \"姓名\",\n" +
//                "                                \"JGGJ_s\": \"籍贯国籍\"\n" +
//                "                            },\n" +
//                "                            \"tableDisplayName\": \"常住人口\",\n" +
//                "                            \"tableConnect\": \"solr://172.30.10.31:7181,172.30.10.32:7181,172.30.10.33:7181/solr\",\n" +
//                "                            \"queryParam\": \"\",\n" +
//                "                            \"mainFlag\": \"1\",\n" +
//                "                            \"tableName\": \"zhk_jczyk_czrk\"\n" +
//                "                        }\n" +
//                "                    ]\n" +
//                "                },\n" +
//                "                {\n" +
//                "                    \"type\": \"0\",\n" +
//                "                    \"nodes\": [\n" +
//                "                        {\n" +
//                "                            \"idDefineName\": \"SFZH_s\",\n" +
//                "                            \"selectSize\": \"200\",\n" +
//                "                            \"fieldSet\": {\n" +
//                "                                \"AJLB_FORMAT_s\": \"案件类别\",\n" +
//                "                                \"ZTLX_FORMAT_s\": \"在逃类型\"\n" +
//                "                            },\n" +
//                "                            \"tableDisplayName\": \"在逃人员_1219\",\n" +
//                "                            \"tableConnect\": \"es://es632@172.30.10.31:29300/ztry_1219\",\n" +
//                "                            \"queryParam\": \"\",\n" +
//                "                            \"mainFlag\": \"0\",\n" +
//                "                            \"tableName\": \"ztry_1219_01\"\n" +
//                "                        }\n" +
//                "                    ]\n" +
//                "                }\n" +
//                "            ]\n" +
//                "        }\n" +
//                "    ]\n" +
//                "}");


// 三条数据测试
//        quest.setNodes("{\n" +
//                "    \"nodes\": [\n" +
//                "        {\n" +
//                "            \"type\": \"1\",\n" +
//                "            \"nodes\": [\n" +
//                "                {\n" +
//                "                    \"type\": \"0\",\n" +
//                "                    \"nodes\": [\n" +
//                "                        {\n" +
//                "                            \"idDefineName\": \"GMSFHM_s\",\n" +
//                "                            \"selectSize\": \"200\",\n" +
//                "                            \"fieldSet\": {\n" +
//                "                                \"XM_s\": \"姓名\",\n" +
//                "                                \"JGGJ_s\": \"籍贯国籍\"\n" +
//                "                            },\n" +
//                "                            \"tableDisplayName\": \"常住人口\",\n" +
//                "                            \"tableConnect\": \"solr://172.30.10.31:7181,172.30.10.32:7181,172.30.10.33:7181/solr\",\n" +
//                "                            \"queryParam\": \"\",\n" +
//                "                            \"mainFlag\": \"1\",\n" +
//                "                            \"tableName\": \"zhk_jczyk_czrk\"\n" +
//                "                        }\n" +
//                "                    ]\n" +
//                "                },\n" +
//                "                {\n" +
//                "                    \"type\": \"0\",\n" +
//                "                    \"nodes\": [\n" +
//                "                        {\n" +
//                "                            \"idDefineName\": \"SFZH_s\",\n" +
//                "                            \"selectSize\": \"200\",\n" +
//                "                            \"fieldSet\": {\n" +
//                "                                \"AJLB_FORMAT_s\": \"案件类别\",\n" +
//                "                                \"ZTLX_FORMAT_s\": \"在逃类型\"\n" +
//                "                            },\n" +
//                "                            \"tableDisplayName\": \"在逃人员_1219\",\n" +
//                "                            \"tableConnect\": \"es://es632@172.30.10.31:29300/ztry_1219\",\n" +
//                "                            \"queryParam\": \"\",\n" +
//                "                            \"mainFlag\": \"0\",\n" +
//                "                            \"tableName\": \"ztry_1219_01\"\n" +
//                "                        }\n" +
//                "                    ]\n" +
//                "                }\n" +
//                "            ]\n" +
//                "        },\n" +
//                "        {\n" +
//                "            \"type\": \"0\",\n" +
//                "            \"nodes\": [\n" +
//                "                {\n" +
//                "                    \"type\": \"0\",\n" +
//                "                    \"nodes\": [\n" +
//                "                        {\n" +
//                "                            \"idDefineName\": \"ZJHM_s\",\n" +
//                "                            \"selectSize\": \"200\",\n" +
//                "                            \"fieldSet\": {\n" +
//                "                                \"BRLXDH_s\": \"本人联系电话\",\n" +
//                "                                \"ZSDPZL_FORMAT_s\": \"注射毒品种类\",\n" +
//                "                                \"SDRYLX_FORMAT_s\": \"涉毒人员类型\"\n" +
//                "                            },\n" +
//                "                            \"tableDisplayName\": \"涉毒人员\",\n" +
//                "                            \"tableConnect\": \"solr://172.30.10.31:7181,172.30.10.32:7181,172.30.10.33:7181/solr\",\n" +
//                "                            \"queryParam\": \"\",\n" +
//                "                            \"mainFlag\": \"0\",\n" +
//                "                            \"tableName\": \"zhk_jczyk_sdry\"\n" +
//                "                        }\n" +
//                "                    ]\n" +
//                "                }\n" +
//                "            ]\n" +
//                "        }\n" +
//                "    ]\n" +
//                "}");

//       更新后的nodes   4条数据
       quest.setNodes("{\n" +
               "    \"nodes\": [\n" +
               "        {\n" +
               "            \"type\": \"1\",\n" +
               "            \"nodes\": [\n" +
               "                {\n" +
               "                    \"type\": \"0\",\n" +
               "                    \"nodes\": [\n" +
               "                        {\n" +
               "                            \"idDefineName\": \"GMSFHM_s\",\n" +
               "                            \"selectSize\": \"200\",\n" +
               "                            \"fieldSet\": {\n" +
               "                                \"XM_s\": \"姓名\",\n" +
               "                                \"JGGJ_s\": \"籍贯国籍\"\n" +
               "                            },\n" +
               "                            \"tableDisplayName\": \"常住人口\",\n" +
               "                            \"tableConnect\": \"solr://172.30.10.31:7181,172.30.10.32:7181,172.30.10.33:7181/solr\",\n" +
               "                            \"queryParam\": \"\",\n" +
               "                            \"mainFlag\": \"1\",\n" +
               "                            \"tableName\": \"zhk_jczyk_czrk\"\n" +
               "                        }\n" +
               "                    ]\n" +
               "                },\n" +
               "                {\n" +
               "                    \"type\": \"0\",\n" +
               "                    \"nodes\": [\n" +
               "                        {\n" +
               "                            \"idDefineName\": \"SFZH_s\",\n" +
               "                            \"selectSize\": \"200\",\n" +
               "                            \"fieldSet\": {\n" +
               "                                \"AJLB_FORMAT_s\": \"案件类别\",\n" +
               "                                \"ZTLX_FORMAT_s\": \"在逃类型\"\n" +
               "                            },\n" +
               "                            \"tableDisplayName\": \"在逃人员_1219\",\n" +
               "                            \"tableConnect\": \"es://es632@172.30.10.31:29300/ztry_1219\",\n" +
               "                            \"queryParam\": \"\",\n" +
               "                            \"mainFlag\": \"0\",\n" +
               "                            \"tableName\": \"ztry_1219_01\"\n" +
               "                        }\n" +
               "                    ]\n" +
               "                }\n" +
               "            ]\n" +
               "        },\n" +
               "        {\n" +
               "            \"type\": \"1\",\n" +
               "            \"nodes\": [\n" +
               "                {\n" +
               "                    \"type\": \"0\",\n" +
               "                    \"nodes\": [\n" +
               "                        {\n" +
               "                            \"idDefineName\": \"ZJHM_s\",\n" +
               "                            \"selectSize\": \"200\",\n" +
               "                            \"fieldSet\": {\n" +
               "                                \"BRLXDH_s\": \"本人联系电话\",\n" +
               "                                \"ZSDPZL_FORMAT_s\": \"注射毒品种类\",\n" +
               "                                \"SDRYLX_FORMAT_s\": \"涉毒人员类型\"\n" +
               "                            },\n" +
               "                            \"tableDisplayName\": \"涉毒人员\",\n" +
               "                            \"tableConnect\": \"solr://172.30.10.31:7181,172.30.10.32:7181,172.30.10.33:7181/solr\",\n" +
               "                            \"queryParam\": \"\",\n" +
               "                            \"mainFlag\": \"0\",\n" +
               "                            \"tableName\": \"zhk_jczyk_sdry\"\n" +
               "                        }\n" +
               "                    ]\n" +
               "                },\n" +
               "                {\n" +
               "                    \"type\": \"0\",\n" +
               "                    \"nodes\": [\n" +
               "                        {\n" +
               "                            \"idDefineName\": \"ZJHM_s\",\n" +
               "                            \"selectSize\": \"200\",\n" +
               "                            \"fieldSet\": {\n" +
               "                                \"LGDZ_s\": \"旅馆地址\",\n" +
               "                                \"ZJHM_s\": \"证件号码\"\n" +
               "                            },\n" +
               "                            \"tableDisplayName\": \"旅馆住宿信息\",\n" +
               "                            \"tableConnect\": \"es://es632@172.30.10.31:29300/lgzsxx_1219\",\n" +
               "                            \"queryParam\": \"\",\n" +
               "                            \"mainFlag\": \"0\",\n" +
               "                            \"tableName\": \"lgzsxx_1219_01\"\n" +
               "                        }\n" +
               "                    ]\n" +
               "                }\n" +
               "            ]\n" +
               "        }\n" +
               "    ]\n" +
               "}");
        try {
            collisionFacade.submit(quest);
            collisionFacade.query(quest, 1, 10);
        } catch (BusinessException e) {
            e.printStackTrace();
        }
    }
}
