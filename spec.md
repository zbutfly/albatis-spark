# SPARK集成

## 开发规范

### 定义

- group: com.hzcominfo.dataggr.spark.integrate
- artifact: spark-integrate
- version: 0.0.1-SNAPSHOT

#### 部件

- spark-integrate-core：核心依赖包，创建spark会话、关闭会话
- 集成模块：
	- spark-integrate-kafka：kafka组装配置，读取数据
	- spark-integrate-mongo：mongo组装配置，读取数据
	- spark-integrate-rdbms：rdbms组装配置，读取数据
	- spark-integrate-hdfs: hdfs组装配置，读取数据
	

#### 代码

##### 代码管理

首个版本开发阶段，使用dataggr仓库组、dataggr仓库，lib-spark-integrate目录。

开发完成首个测试，发布RC版本，切换到独立仓库，作为独立框架组件。

##### 项目结构

- lib-spark-integrate/pom.xml
- lib-spark-integrate/core/pom.xml: spark-integrate-core
- lib-spark-integrate/kafka/pom.xml: spark-integrate-kafka
- lib-spark-integrate/mongo/pom.xml: spark-integrate-mongo
- lib-spark-integrate/rdbms/pom.xml: spark-integrate-rdbms
- lib-spark-integrate/hdfs/pom.xml: spark-integrate-hdfs

### 构建

统一使用maven构建。
产品应继承com.hzcominfo:parent:3.0.0-SNAHSHOT父pom。
构建结果发布匡信产品仓库，使用定义的GAV结构。

## 总体设计

SPARK集成作为各类数据处理的框架，主要为pump项目提供统一的数据输入，以及数据之间相互碰撞。应**逐步**支持以下功能：
1. 提供各类数据的读取配置
2. 读取各类数据
3. 统一各类数据输出

## 接口定义

### 读取数据

1. 读取配置
2. 读取统一的数据输入

### 数据碰撞

根据参与碰撞的表信息及碰撞类型，提供数据碰撞任务接口。

- 输入参数
	1. 参与碰撞的表信息
	2. 碰撞类型
- 返回结果：根据不同的接口以及相应参数，返回对应的任务编号、任务状态或者任务结果
	- 任务编号
	- 任务状态
	- 任务结果

## 技术架构

![技术架构《arch.vsdx》](docs/arch-large.jpg)