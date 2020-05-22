package baicdt.bigdata.pkg
import java.sql.{Connection, DriverManager}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.util.Properties

import org.apache.spark.sql.SaveMode

//按日 月 周 小时统计接入任务类型
class statisTaskType {
  def runstatisTaskType(): Unit = {



    System.setProperty("HADOOP_USER_NAME", "hdfs")
    System.setProperty("user.name", "hdfs")




    //try{
    var v_date =""

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("spark-sql-demo")
      .master("local[*]")
      //.master("yarn")
      .config("hive.metastore.uris", "thrift://ambari03.baicdt.com:9083")
      .config("dfs.client.use.datanode.hostname", "true")//通过域名来访问HIVE,不是通过内网IP，因为本地程序和hadoop不是在一个网段，本地程序通过VPN访问的hadoop集群
      .config("dfs.datanode.use.datanode.hostname", "true") //通过域名来访问HIVE,不是通过内网IP，因为本地程序和hadoop不是在一个网段，本地程序通过VPN访问的hadoop集群
      .config("metastore.catalog.default", "hive")  //指定spark-sql读hive元数据，默认值是"spark"，表示读spark-sql本地库，spark内置hive,外接hive库时需改此参数
      .config("spark.sql.warehouse.dir", "hdfs://ambari01.baicdt.com:8020/warehouse/tablespace/managed/hive") //指定hive的warehouse目录
      .config("hive.strict.managed.tables","false").config("hive.create.as.insert.only","false")
      .config("metastore.create.as.acid","false")// 直接连接hive
      .enableHiveSupport()
      .getOrCreate()

    //hive 3.0之后默认开启ACID功能，而且新建的表默认是ACID表。而spark目前还不支持hive的ACID功能，因此无法读取ACID表的数据
    //修改以下参数让新建的表默认不是acid表。
    //只有关掉hive3.x表的acid后，spark-sql才能正常读取hive表中的数据.
    //hive.strict.managed.tables=false
    //hive.create.as.insert.only=false
    //metastore.create.as.acid=false

    //Thread.sleep(100L)  //休眠100毫秒，L毫秒
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("WARN")
    //import spark.implicits._
    //val lineRDD: RDD[String] = sc.textFile("hdfs://ambari01.baicdt.com:8020/test/spark/words.txt")
    //val rsRDD: RDD[(String, Int)] = lineRDD.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)











    ////任务类型统计模块////
    //连接前端WEB MYSQL数据库
    val url3 = "jdbc:mysql://flink01.baicdt.com:3306/bigdata_sys"
    var sql3=""
    //数据入库，需要new一个Properties方法
    val conn_info3 = new Properties()

    //获取数据库的用户名，密码和运行的driver类
    conn_info3.setProperty("user", "root")
    conn_info3.setProperty("password", "123456")
    //conn_info2.setProperty("user", "test")
    //conn_info2.setProperty("password", "test")
    conn_info3.setProperty("driver", "com.mysql.jdbc.Driver")

    sparkSession.sql("use whdb")
    //从WEB MYSQL 库中读取接入任务表
    val rs_t_imp_task_df = sparkSession.read.format("jdbc").jdbc(url3, "t_imp_task", conn_info3)
    rs_t_imp_task_df.show()
    rs_t_imp_task_df.createOrReplaceTempView("t_imp_task_tmp")
    sparkSession.sql("truncate table whdb.t_imp_task")
    sparkSession.sql("insert into whdb.t_imp_task select * from t_imp_task_tmp")
    var rddf=sparkSession.sql("select count(1)  cn from whdb.t_imp_task")
    rddf.show()

    //从WEB MYSQL 库中读取字典表
    val rs_dic_code_df = sparkSession.read.format("jdbc").jdbc(url3, "dic_code", conn_info3)
    rs_dic_code_df.show()
    rs_dic_code_df.createOrReplaceTempView("t_dic_code_tmp")
    sparkSession.sql("truncate table whdb.dic_code")
    sparkSession.sql("insert into whdb.dic_code select * from t_dic_code_tmp")
    rddf=sparkSession.sql("select count(1)  cn from whdb.dic_code")
    rddf.show()
    //var rs_tmp_df=sparkSession.sql("select * from whdb.dic_code")
    //rs_tmp_df.createOrReplaceTempView("tmp_dic_code")
    //rs_tmp_df=sparkSession.sql("select * from whdb.t_imp_task")
    //rs_tmp_df.createOrReplaceTempView("tmp_t_imp_task")

    //按天统计生成任务类型结果表
    var rs_result1=sparkSession.sql("select * from whdb.dic_code")
    var v_date2="2020-05-13"

    //按天统计任务类型
    var sql4=" "+
      " insert into t_result_day_task_type " +
      " select  to_date(to_timestamp(task_start_time,'yyyy-MM-dd HH24:mi:ss') ,'yyyy-MM-dd'), "+
      " m.task_schedule_type, "+
      " n.dic_name, "+
      " count(1) cn "+
      "  from t_imp_task m, "+
      " dic_code n "+
      "   where m.task_schedule_type=n.dic_code "+
      " and substr(m.task_start_time,1,10)= '" +v_date2 +  "'"+
      " group by to_date(to_timestamp(task_start_time,'yyyy-MM-dd HH24:mi:ss') ,'yyyy-MM-dd'), " +
      " m.task_schedule_type, "+
      " n.dic_name "
    rs_result1=sparkSession.sql(sql4)

    sparkSession.sql("refresh table t_result_day_task_type")


    //按月统计任务类型
    var v_mon="2020-05"
    sql4=" "+
      " insert into t_result_mon_task_type " +
      " select  substr(task_start_time,1,7), "+
      " m.task_schedule_type, "+
      " n.dic_name, "+
      " count(1) cn "+
      "  from t_imp_task m, "+
      " dic_code n "+
      "   where m.task_schedule_type=n.dic_code "+
      " and substr(task_start_time,1,7)= '" +v_mon +  "'"+
      " group by substr(task_start_time,1,7), " +
      " m.task_schedule_type, "+
      " n.dic_name "
    rs_result1=sparkSession.sql(sql4)


    //按周统计任务类型
    var v_week="202020"
    sql4=" "+
      " insert into t_result_week_task_type " +
      " select  substr(task_start_time,1,4)||"+"weekofyear(task_start_time), "+
      " m.task_schedule_type, "+
      " n.dic_name, "+
      " count(1) cn "+
      "  from t_imp_task m, "+
      " dic_code n "+
      "   where m.task_schedule_type=n.dic_code "+
      " and substr(task_start_time,1,4)||"+"weekofyear(task_start_time)= '" +v_week +  "'"+
      " group by substr(task_start_time,1,4)||"+"weekofyear(task_start_time), " +
      " m.task_schedule_type, "+
      " n.dic_name "
    rs_result1=sparkSession.sql(sql4)

    var v_hour="2020-05-13 19"
    //按小时统计任务类型
    sql4=" "+
      " insert into t_result_hour_task_type " +
      " select  substr(task_start_time,1,13) , "+
      " m.task_schedule_type, "+
      " n.dic_name, "+
      " count(1) cn "+
      "  from t_imp_task m, "+
      " dic_code n "+
      "   where m.task_schedule_type=n.dic_code "+
      " and substr(task_start_time,1,13)= '" +v_hour +  "'"+
      " group by substr(task_start_time,1,13), " +
      " m.task_schedule_type, "+
      " n.dic_name "
    rs_result1=sparkSession.sql(sql4)


    //获取web端MYSQL bigdata_sys数据库 连接

    val url_bigdata_sys = "jdbc:mysql://flink01.baicdt.com:3306/bigdata_sys"
    //val driver = "com.mysql.jdbc.Driver"
    //数据入库，需要new一个Properties方法
    val conn_info_bigdata_sys = new Properties()

    //获取数据库的用户名，密码和运行的driver类
    conn_info_bigdata_sys.setProperty("user", "root")
    conn_info_bigdata_sys.setProperty("password", "123456")
    //conn_info.setProperty("user", "test")
    //conn_info.setProperty("password", "test")
    conn_info_bigdata_sys.setProperty("driver", "com.mysql.jdbc.Driver")
    //将spark-sql生成的报表结果表同步至BI报表库。
    //从HIVE库同步任务类型统计日报至MYSQL库
    sql4="select * from whdb.t_result_day_task_type"
    var rs_result_mid_df=sparkSession.sql(sql4)
    rs_result_mid_df.write.mode(SaveMode.Append).jdbc(url_bigdata_sys,"t_result_day_task_type",conn_info_bigdata_sys)

    //从HIVE库同步任务类型统计月报至MYSQL库
    sql4="select * from whdb.t_result_mon_task_type"
    rs_result_mid_df=sparkSession.sql(sql4)
    rs_result_mid_df.write.mode(SaveMode.Append).jdbc(url_bigdata_sys,"t_result_mon_task_type",conn_info_bigdata_sys)


    //从HIVE库同步任务类型统计周报至MYSQL库
    sql4="select * from whdb.t_result_week_task_type"
    rs_result_mid_df=sparkSession.sql(sql4)
    rs_result_mid_df.write.mode(SaveMode.Append).jdbc(url_bigdata_sys,"t_result_week_task_type",conn_info_bigdata_sys)


    //从HIVE库同步任务类型统计日报至MYSQL库
    sql4="select * from whdb.t_result_hour_task_type"
    rs_result_mid_df=sparkSession.sql(sql4)
    rs_result_mid_df.write.mode(SaveMode.Append).jdbc(url_bigdata_sys,"t_result_hour_task_type",conn_info_bigdata_sys)










  }
}
