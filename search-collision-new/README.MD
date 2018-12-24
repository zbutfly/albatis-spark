## 搜索碰撞
### search-collision-new

#### 碰撞任务建表
1. COLLISION （碰撞任务表）
	~~~sql
		create table COLLISION (
			ID int(24) primary key not null auto_increment comment '主键',
			TASK_ID varchar(100) comment '碰撞任务编号',
			USER_NAME varchar(100) comment '碰撞发起用户名',
			PKI_NAME varchar(100) comment '碰撞发起PKI名称',
			TASK_TYPE char(1) comment '碰撞任务类型 1：并且，2：或者，3：非；目前仅有1',
			TASK_STATE varchar(100) comment '碰撞任务状态 WAITING, RUNNING, SUCCESSED, FAILED, CANCEL, BAD',
			REMARK varchar(10000) comment '标记内容',
			CONDITIONS varchar(10000) comment '碰撞任务条件',
			UPDATE_TIME varchar(100) comment '更新时间',
			ADD_TIME varchar(100) comment '添加时间'
		)
	~~~
1. COLLISION_REQUEST （碰撞任务详表）
	~~~sql
		create table COLLISION_REQUEST (
			ID int(24) primary key not null auto_increment comment '主键',
			TASK_ID varchar(100) comment '碰撞任务编号',
			MAIN_FLAG varchar(3) comment '碰撞主表标识 0：主表，其他：非主表',
			TABLE_NAME varchar(100) comment '碰撞表名',
			TABLE_DISPLAY_NAME varchar(100) comment '碰撞表物理名',
			TABLE_CONNECT varchar(100) comment '碰撞表连接',
			SELECT_SIZE varchar(100) comment '碰撞表所选数据大小',
			ID_DEFINE_NAME varchar(100) comment '碰撞字段',
			QUERY_PARAM varchar(10000) comment 'SQL查询条件部分',
			FIELD_SET varchar(1000) comment '字段说明集合',
			ADD_TIME varchar(100) comment '添加时间'
		)
	~~~

#### 碰撞请求参数结构示例
	~~~json
		{
			'userName':'chenw',
			'pkiName':'111111',
			'type':'1',
			'conditions':[
				{
					'mainFlag':'0',
					'tableName':'ORACLE_MONGO_CZRK',
					'tableDisplayName':'常住人口',
					'tableConnect':'zk://data01:7181,data02:7181,data03:7181',
					'queryParam':'limit 100',
					'idDefineName':'GMSFHM_s',
					'fieldSet':{'GMSFHM_s':'证件号码','HJDZ_XZ_s':'现住地址'},
					'selectSize':'200'
				},
				{
					'mainFlag':'1',
					'tableName':'ES_ORACLE_MONGO_CZRK',
					'tableDisplayName':'ES常住人口',
					'tableConnect':'es://hzcominfo@172.30.10.31:39300/dpc_test?httpport=39200',
					'queryParam':'where GMSFHM_s like \'%330%\' ',
					'idDefineName':'GMSFHM_s',
					'fieldSet':{'GMSFHM_s':'证件号码','JG_SSXQ_FORMAT_s':'现住地址'},
					'selectSize':'200'
				}
			]
		}
	~~~

#### 接口服务调用测试用例 （待修改...）

1. SEARCH_COLLISION_001 （新增碰撞任务）
	~~~curl
	
		curl -d  "[{'userName':'chenw','pkiName':'111111','type':'1','conditions':\"[{'mainFlag':'0','tableName':'ORACLE_MONGO_CZRK','tableDisplayName':'常住人口','tableConnect':'zk://data01:7181,data02:7181,data03:7181','queryParam':'limit 200','idDefineName':'GMSFHM_s','fieldSet':{'GMSFHM_s':'证件号码','HJDZ_XZ_s':'现住地址'},'selectSize':'200'},{'mainFlag':'1','tableName':'ES_ORACLE_MONGO_CZRK','tableDisplayName':'ES常住人口','tableConnect':'es://hzcominfo@172.30.10.31:39300/dpc_test?httpport=39200','queryParam':'limit 200','idDefineName':'GMSFHM_s','fieldSet':{'GMSFHM_s':'证件号码','JG_SSXQ_FORMAT_s':'现住地址'},'selectSize':'200'}]\"}]" -H "Content-Type:application/json,charset=UTF-8" http://172.16.17.32:49900/search-collision-new/SEARCH_COLLISION_001
	~~~
	
1. SEARCH_COLLISION_002 (查询碰撞结果)
	~~~curl
		curl -d "[{'taskId':'120091191978525'}, 1, 10]" -H "Content-Type:application/json,charset=UTF-8" http://172.16.17.32:49900/search-collision-new/SEARCH_COLLISION_002
	~~~

1. SEARCH_COLLISION_003 (取消碰撞)
	~~~curl
		curl -d "[{'taskId':'149123812325716'}]" -H "Content-Type:application/json,charset=UTF-8" http://172.16.17.32:49900/search-collision-new/SEARCH_COLLISION_003
	~~~

1. SEARCH_COLLISION_004 (导出碰撞)
	~~~curl
	
		curl -d "[{'taskId':'149123812325716'}]" -H "Content-Type:application/json,charset=UTF-8" http://172.16.17.32:49900/search-collision-new/SEARCH_COLLISION_004
	~~~
	
	
	
	
	
	