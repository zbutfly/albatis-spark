package com.hzcominfo.search.collision.impl.facade;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.*;
import com.hzcominfo.dataggr.spark.io.SparkSchedule;
import com.hzcominfo.search.collision.CollisionExecutorCache;
import com.hzcominfo.search.collision.CollisionResult;
import com.hzcominfo.search.collision.InitThreadImpl;
import com.hzcominfo.search.collision.dto.CollisionResponse;
import com.hzcominfo.search.collision.dto.Collisionquest;
import com.hzcominfo.search.collision.facade.CollisionFacade;
import com.hzcominfo.search.collision.mapper.Collision;
import com.hzcominfo.search.collision.mapper.Collision.CollisionState;
import com.hzcominfo.search.collision.mapper.CollisionReq;
import com.hzcominfo.search.collision.util.Constants;
import com.hzcominfo.search.collision.util.ExportExcelFile;
import com.hzcominfo.search.collision.util.JsonUtils;
import com.hzcominfo.search.collision.util.Strs;
import net.butfly.albacore.exception.BusinessException;
import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.facade.FacadeBase;
import net.butfly.albacore.utils.logger.Logger;
import org.springframework.beans.BeanUtils;
import scala.Tuple2;

public class CollisionFacadeImpl extends FacadeBase implements CollisionFacade {
    private static final long serialVersionUID = 6833213909099199024L;
    static Logger log = Logger.getLogger(CollisionFacadeImpl.class);


    @Override
//  碰撞任务记录到mysql表中,其他也没干啥
    public synchronized CollisionResponse submit(Collisionquest model) throws BusinessException {
        CollisionResponse resp = new CollisionResponse();
        Gson gson = new Gson();

        if (model == null)
            return resp.setCode(404).setMsg("请求参数为空");

        if (model.getNodes() == null)
            return resp.setCode(404).setMsg("请传入最新的参数结构");

        Date date = new Date();
        String fdate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);

        String rootNodes = model.getNodes();

//      开始解析这个gson串到Collision对象

//      -----开始解析复杂json,在解析过程中去调用join方法或转input----------------------------------
        List<List<List<JsonElement>>> list = new ArrayList<>();
        JsonElement nodesJson = gson.fromJson(rootNodes, JsonElement.class);

        //		业务要求用key,就当他还是原来的taskId
        String taskId = model.getTaskKey();

        List<JsonElement> parents = new ArrayList<>();

        parents.add(nodesJson);
//        JsonElement nodes1 = nodesJson.getAsJsonObject().get("nodes"); 队列里要放两个属性,
        LinkedBlockingQueue<Tuple2<JsonElement, JsonElement>> queue = new LinkedBlockingQueue<>();
        int currentNodeNum = 1;
        try {
            //1.parent 2.当前
            Tuple2<JsonElement, JsonElement> node = new Tuple2<>(nodesJson, nodesJson);
            queue.put(node);
//          当队列空时,退出循环
            while (!queue.isEmpty()) {
//              删除队列头部的元素,并返回
                Tuple2<JsonElement, JsonElement> element = queue.poll();

                JsonElement parentNode = element._1;
                JsonElement currentNode = element._2;
//              如果
                if (currentNode.isJsonObject()) {
                    if (currentNode.getAsJsonObject().get("nodes") != null) {
                        queue.put(new Tuple2<>(parentNode, currentNode.getAsJsonObject().get("nodes")));
                    } else {
//                      走到这的都是{},下边只有{}数据
                        CollisionReq req = new CollisionReq();
                        changeFieldSetType(currentNode);
                        CollisionReq current = new Gson().fromJson(currentNode, CollisionReq.class);
                        BeanUtils.copyProperties(current, req);
                        req.setAddTime(fdate);
                        req.setTaskId(taskId);
                        req.setHermano("兄弟表名");
                        logger.debug("data -->" + JSON.toJSONString(req));
                    }
                }
                if (currentNode.isJsonArray()) {
                    List<List<JsonElement>> list2 = new ArrayList<>();
                    List<JsonElement> list3 = new ArrayList<>();
                    int size = currentNode.getAsJsonArray().size();
//                  要遍历当前节点, 第一次进来的是根node[]
                    currentNode.getAsJsonArray().forEach(obj -> {
                        try {
                            list3.add(obj);
                            Tuple2<JsonElement, JsonElement> tuple2 = new Tuple2<>(currentNode, obj);
                            queue.put(new Tuple2<>(currentNode, obj));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
                    list2.add(list3);
                    //判断是否同级
                    if (!parents.contains(parentNode)) {
                        currentNodeNum++;
                        list.add(list2);
                        parents.add(currentNode); //当前父亲节点
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//      加载到数据库去做顺序的join
        for (int i = 0; i < list.size(); i++) {
            for (int j = 0; j < list.get(i).size(); j++) {
                List<JsonElement> elements = list.get(i).get(j);
                List<CollisionReq> reqList = new ArrayList<>();
                List<CollisionReq> cReqs = new ArrayList<>();
                elements.forEach(item -> {
                    changeFieldSetType(item);
                    CollisionReq req = new Gson().fromJson(item.getAsJsonObject().get("nodes").getAsJsonArray().get(0), CollisionReq.class);
                    req.setAddTime(fdate);
                    req.setTaskId(taskId);
                    reqList.add(req);
                    logger.debug("data -->" + JSON.toJSONString(req));
                    cReqs.add(req);
                });
                if(cReqs.size() > 1) {
                    cReqs.get(0).setHermano(cReqs.get(1).getTableName());
                    cReqs.get(1).setHermano(cReqs.get(0).getTableName());
                }else if (cReqs.size() == 1){
                    cReqs.get(0).setHermano("none");
                }
                Collision collision = new Collision();
                collision.setTaskId(taskId);
                collision.setUserName(model.getUserName());
                collision.setPkiName(model.getPkiName());
                collision.setType(model.getType());
                collision.setState(CollisionState.WAITING.toString());
                collision.setConditions(rootNodes);
                collision.setUpdateTime(fdate);
                collision.setAddTime(fdate);
                collision.setRemark("添加任务成功");

                if (collision != null) {
                    if (cReqs != null && !cReqs.isEmpty())
//              todo 要在这插入hermano的数据
                        for (CollisionReq req : cReqs)
                            CollisionReq.insert(req);
                    Collision.insert(collision);
                }

            }
        }

//      存到CollisionReq数据库
        if (!JsonUtils.isJson(rootNodes))
            return resp.setCode(404).setMsg("请求参数碰撞条件不符合规范");


        resp.setTaskId(taskId);
        resp.setPercentage("0");
        return resp.setCode(200).setMsg("OK");
    }

    private JsonObject jsonArrayToObj(Gson gson, JsonElement jsonElement) {
        String strJ = String.valueOf(jsonElement);
        String objStr = strJ.replaceAll("[\\[\\]]", "");
        JsonElement jsonElement1 = gson.fromJson(objStr, JsonElement.class);
        JsonObject asJsonObject = jsonElement1.getAsJsonObject();
        return asJsonObject;
    }

    private void changeFieldSetType(JsonElement currentNode) {
        JsonElement jsonElement = currentNode.getAsJsonObject().get("fieldSet");
        String strFieldset = String.valueOf(jsonElement);
//                      fieldSet的类型是Bean,要想办法给改成string
        currentNode.getAsJsonObject().remove("fieldSet");
        currentNode.getAsJsonObject().addProperty("fieldSet", strFieldset);
    }


    @Override
    public CollisionResponse query(Collisionquest model, int currPage, int pageSize) throws BusinessException {
        CollisionResponse resp = new CollisionResponse();

//       todo 部署的时候删掉,在shell中调用
        InitThreadImpl initThread = new InitThreadImpl();
        initThread.start();

        if (model == null || model.getNodes() == null || model.getTaskId() == null)
            return resp.setCode(404).setMsg("请求参数为空");

        String taskId = model.getTaskId();
//        String taskKey = model.getTaskKey();
//		拿到taskKey，看是否有这个任务 如果有,任务运行完没有
        Collision collision = Collision.selectByTaskId(taskId);
//      看任务的
        if (collision == null)
            return resp.setCode(400).setMsg("无该会话碰撞").setResultCode(0);
        if (CollisionState.WAITING.toString().equals(collision.getState()))
            return resp.setCode(400)
//                  任务状态,并发量输出到控制台
                    .setMsg("执行中的任务：" + CollisionExecutorCache.getRunning() + ", 碰撞并发容量为：" + InitThreadImpl.cap)
                    .setResultCode(0);
        if (!CollisionState.SUCCESSED.toString().equals(collision.getState())) {
            resp.setResultCode(1);
            int percent = SparkSchedule.calcPercent(taskId);
            if (percent != 100) {
                resp.setPercentage(String.valueOf(percent));
                String msg = "该会话碰撞未完成，当前状态：" + collision.getState();
                return resp.setCode(200).setMsg(msg).setResultCode(1);
            }
        }
//		拿到CollisionReq对象集合,所有的collision信息,这是到req表里取查的
        List<CollisionReq> collisionReqs = CollisionReq.selectCReqByTaskId(taskId);
        List<Map<String, Object>> rows = new ArrayList<>();
        Map<String, Byte> idRowsMap = new HashMap<>();
//
        List<Map<String, Object>> cResults = CollisionResult.fetch(taskId, currPage, pageSize);
        for (CollisionReq cReq : collisionReqs) {
            Map<String, Object> row = new HashMap<>();
            row.put(Constants.MAIN_FLAG, cReq.getMainFlag());
            row.put(Constants.TABLE_NAME, cReq.getTableName());
            row.put(Constants.TABLE_DISPLAY_NAME, cReq.getTableDisplayName());
//			Map里key是一个Map，value是OBJ
            Map<Map<String, Object>, Object> valuesMap = new HashMap<>();
            // {t__o__k:v} -> {k:v}
            cResults.forEach(m -> {
                String resultKey = (String) m.get(Strs.char_concat("__", cReq.getTableName(), cReq.getMainFlag(), cReq.getIdDefineName()));
                Map<String, Object> subMap = new HashMap<>();

                m.forEach((tok, v) -> {
                    String[] split = tok.split("__");
                    subMap.put(split[split.length - 1], v);
                });
                subMap.put("result_key", resultKey);
                valuesMap.put(subMap, 0);
//				0代表这个是主表
                if ("0".equals(cReq.getMainFlag()))
                    idRowsMap.put(resultKey, (byte) 0);
            });
            row.put(Constants.VALUES, valuesMap.keySet());
            rows.add(row);
        }
//		respon要响应join后的数据,展示到linux控制台; 我的结果数据也要展示到这
        resp.setRows(rows);
        resp.setIdRows(idRowsMap.keySet());
//      控制展示数量
        resp.setTotal(idRowsMap.size());
        resp.setRemark(collision.getRemark());
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date beginTime = sdf.parse(collision.getAddTime());
            Date endTime = sdf.parse(collision.getUpdateTime());
            resp.setSpends((endTime.getTime() - beginTime.getTime()) / 1000);
        } catch (ParseException e) {
            log.error("parse time error: " + collision.getUpdateTime());
        }
        return resp.setCode(200).setMsg("OK").setResultCode(2);
    }

//  传入一个node[],返回这个node[]下的主表 主表返回true
//    public static boolean isMainTable(JsonArray nodes){
//        int size = nodes.size();
//        if (size >= 2){
//            nodes.get(0);
//        }
//        return true;
//    }

    //  策略二 直接给他设置好这个array里的主表,副表  nodes[]一个调用一次
    public void setMainTableForArray(JsonArray nodes) {
//      把第一个设置成0,其他设置1  遍历数组,第一个add主表映射,其他add副表映射
//      todo 要求传入的就是一个element(0)
        Gson gson = new Gson();
        for (int i = 0; i < nodes.size(); i++) {
//          这是一个array
            JsonElement jsonElement = nodes.get(i);
            JsonObject obj = jsonElement.getAsJsonObject();
            //          array转obj,用来放元素
            String jsonStr = obj.getAsJsonArray("nodes").toString();
            String str = jsonStr.replaceAll("[\\[\\]]", "");
            JsonElement nodesE = gson.fromJson(str, JsonElement.class);
            JsonObject nodesObj = nodesE.getAsJsonObject();
//          把noMainObj和asJsonObj
            if (0 != i) {
                nodesObj.addProperty("mainFlag", "1");
            } else {
//              设置主表 0代表mainTable
                nodesObj.addProperty("mainFlag", "0");
            }
//            要给这个jsone一个[] 把object搞成array,再给obj加入元素
            String asString = "[" + nodesObj.toString() + "]";
            ;
            obj.add("type", obj.get("type"));
//          要把noMainObj加到obj里作为
            obj.add("nodes", gson.fromJson(asString, JsonElement.class));
        }
    }


    @Override
    public CollisionResponse cancel(Collisionquest model) throws BusinessException {
        CollisionResponse resp = new CollisionResponse();
        if (model == null || model.getTaskId() == null)
            return resp.setCode(404).setMsg("没有定义请求的会话ID 请检查请求参数");
        String taskId = model.getTaskKey();

        InitThreadImpl.endJob(taskId);
        Collision.end(taskId);
        return resp.setCode(200).setMsg("结束碰撞成功 taskID:" + taskId);
    }

    @Override
    public CollisionResponse export(Collisionquest model) throws BusinessException {
        CollisionResponse resp = new CollisionResponse();
        if (model == null || model.getTaskId() == null)
            return resp.setCode(404).setMsg("请求参数为空");
        String taskId = model.getTaskId();
        List<CollisionReq> cReqs = CollisionReq.selectCReqByTaskId(taskId);
        Collection<String> titleStr = new ArrayList<>();
        String sheetName = "碰撞结果";

        // {k:c},t-tn -> {t__o__k:tn__o__c}
        Map<String, String> relMap = new HashMap<>();
        for (CollisionReq cReq : cReqs) {
            String fieldSet = cReq.getFieldSet();
            if (fieldSet != null && !"".equals(fieldSet)) {
                JSONObject json = JSON.parseObject(fieldSet);
                json.forEach((k, v) -> relMap.put(Strs.char_concat("__", cReq.getTableName(), cReq.getMainFlag(), k),
                        Strs.char_concat("__", cReq.getTableDisplayName(), cReq.getMainFlag(), (String) v)));
            }
        }
        titleStr = relMap.values();

        // {t__o__k:v},{t__o__k:tn__o__c} -> {tn__o__c:v}
        List<Map<String, Object>> mapList = new ArrayList<>();
        List<Map<String, Object>> cResults = CollisionResult.fetch(taskId);
        cResults.forEach(m -> {
            Map<String, Object> map = new HashMap<>();
            relMap.forEach((k, v) -> map.put(v, m.get(k)));
            mapList.add(map);
        });

        String[] title = new String[titleStr.size()];
        resp.setFileUrl(commonExport(titleStr.toArray(title), sheetName, mapList, taskId));
        return resp.setCode(200).setMsg("OK");
    }

    private File exportRootFile;
    private String exportRoot;

    public void setExportRoot(String exportRoot) {
        String base = System.getProperty("bus.server.base");
        if (null == base)
            throw new SystemException("", "Result exporting need definition of web base");
        this.exportRoot = ("/" + exportRoot + "/").replaceAll("[\\\\/]+", "/");
        base = (base + this.exportRoot).replaceAll("[\\\\/]+", "/");
        log.info("Search result exporting use the path as temp file location: " + base);
        this.exportRootFile = new File(base);
        this.exportRootFile.mkdirs();
    }

    private String commonExport(String[] title, String sheetName, List<Map<String, Object>> mapList, String fileName) {
        File temp = new File(this.exportRootFile.getAbsoluteFile() + "/" + fileName + ".xlsx");
        ExportExcelFile ee = new ExportExcelFile();
        ee.exportExcelFile(title, sheetName, mapList);
        ee.writeFile(this.exportRootFile + "//" + temp.getName());
        return this.exportRoot + temp.getName();
    }
}
