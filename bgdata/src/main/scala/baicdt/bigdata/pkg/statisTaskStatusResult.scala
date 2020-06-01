package baicdt.bigdata.pkg

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

//按日 月 周 小时统计接入任务状态
class statisTaskStatusResult {
  //按日 月 周 小时统计接入任务状态
  def runStatisTaskStatus(f_time_type:String){

    //异常处理
    try {

      System.setProperty("HADOOP_USER_NAME", "hdfs")
      System.setProperty("user.name", "hdfs")
      var v_time_type = f_time_type
      var v_time = ""
      //包含day mon week hour四种类型，放在一个表中t_task_time_status
      var v_date2 = "2020-05-13"
      var v_mon = "2020-05"
      var v_week = "202020"
      var v_hour = "2020-05-13 19"
      var rs_result_mid_df: DataFrame = null
      var sql5=""

      //连接前端WEB MYSQL数据库
      val url3 = "jdbc:mysql://flink01.baicdt.com:3306/bigdata_sys"
      var sql3 = ""
      //数据入库，需要new一个Properties方法
      val conn_info3 = new Properties()

      //获取数据库的用户名，密码和运行的driver类
      conn_info3.setProperty("user", "root")
      conn_info3.setProperty("password", "123456")
      //conn_info2.setProperty("user", "test")
      //conn_info2.setProperty("password", "test")
      conn_info3.setProperty("driver", "com.mysql.jdbc.Driver")


      //获取mysql库中每天该处理那天的报表
      var connection: Connection = null
      classOf[com.mysql.jdbc.Driver]
      connection = DriverManager.getConnection(url3, conn_info3)
      //val sql = s"update test_data.mysql_stu_info set age = $age where name='$name'"
      var sql = s"select min(t.statis_time) statis_time from t_task_time_status t where t.status ='none' and t.time_type='$v_time_type'"
      var rs1 = connection.createStatement().executeQuery(sql)

      while (rs1.next()) {
        v_time = rs1.getString("statis_time")
        if (v_time==null) {sys.exit(-1)}
      }

      if (v_time_type == "day") {
        v_date2 = v_time
      }
      else if (v_time_type == "mon") {
        v_mon = v_time
      }
      else if (v_time_type == "week") {
        v_week = v_time
      }
      else if (v_time_type == "hour") {
        v_hour = v_time
      }
      else {
        println("日期:" + v_time + "获取不正确，请重新输入正确的日期")
        println("请输入统计维度参数 日 day 月 mon 周 week 时 hour")
        sys.exit(-1)
      }


      val sparkSession = SparkSession.builder()
        .appName("spark-sql-demo")
        .master("local[*]")
        //.master("yarn")
        .config("hive.metastore.uris", "thrift://ambari03.baicdt.com:9083")
        .config("dfs.client.use.datanode.hostname", "true") //通过域名来访问HIVE,不是通过内网IP，因为本地程序和hadoop不是在一个网段，本地程序通过VPN访问的hadoop集群
        .config("dfs.datanode.use.datanode.hostname", "true") //通过域名来访问HIVE,不是通过内网IP，因为本地程序和hadoop不是在一个网段，本地程序通过VPN访问的hadoop集群
        .config("metastore.catalog.default", "hive") //指定spark-sql读hive元数据，默认值是"spark"，表示读spark-sql本地库，spark内置hive,外接hive库时需改此参数
        .config("spark.sql.warehouse.dir", "hdfs://ambari01.baicdt.com:8020/warehouse/tablespace/managed/hive") //指定hive的warehouse目录
        .config("hive.strict.managed.tables", "false").config("hive.create.as.insert.only", "false")
        .config("metastore.create.as.acid", "false") // 直接连接hive
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


      ////任务状态统计模块////


      sparkSession.sql("use whdb")







      //从WEB MYSQL 库中读取接入任务表
      val rs_t_imp_task_df = sparkSession.read.format("jdbc").jdbc(url3, "t_imp_task", conn_info3)
      rs_t_imp_task_df.show()
      rs_t_imp_task_df.createOrReplaceTempView("t_imp_task_tmp")
      sparkSession.sql("truncate table whdb.t_imp_task")
      sparkSession.sql("insert into whdb.t_imp_task select * from t_imp_task_tmp")
      var rddf = sparkSession.sql("select count(1)  cn from whdb.t_imp_task")
      //rddf.show()

      //从WEB MYSQL 库中读取字典表
      val rs_dic_code_df = sparkSession.read.format("jdbc").jdbc(url3, "dic_code", conn_info3)
      rs_dic_code_df.show()
      rs_dic_code_df.createOrReplaceTempView("t_dic_code_tmp")
      sparkSession.sql("truncate table whdb.dic_code")
      sparkSession.sql("insert into whdb.dic_code select * from t_dic_code_tmp")
      rddf = sparkSession.sql("select count(1)  cn from whdb.dic_code")
      //rddf.show()
      //var rs_tmp_df=sparkSession.sql("select * from whdb.dic_code")
      //rs_tmp_df.createOrReplaceTempView("tmp_dic_code")
      //rs_tmp_df=sparkSession.sql("select * from whdb.t_imp_task")
      //rs_tmp_df.createOrReplaceTempView("tmp_t_imp_task")

      //按天统计生成任务类型结果表
      var rs_result1 = sparkSession.sql("select * from whdb.dic_code")


      //按天统计任务类型
      sparkSession.sql("truncate table  whdb.t_result_day_task_status")
      var sql4 = " " +
        " insert into t_result_day_task_status " +
        " select  to_date(to_timestamp(status_update_time,'yyyy-MM-dd HH24:mi:ss') ,'yyyy-MM-dd'), " +
        " m.task_status, " +
        " n.dic_name, " +
        " count(1) cn " +
        "  from t_imp_task m, " +
        " dic_code n " +
        "   where m.task_status=n.dic_code " +
        " and substr(m.status_update_time,1,10)= '" + v_date2 + "'" +
        " group by to_date(to_timestamp(status_update_time,'yyyy-MM-dd HH24:mi:ss') ,'yyyy-MM-dd'), " +
        " m.task_status, " +
        " n.dic_name "

      //按天统计
      if (v_time_type == "day") {
        sql5 = s"delete from t_result_day_task_status  where sta_time ='$v_time'"
        connection.createStatement().executeUpdate(sql5)
        rs_result1 = sparkSession.sql(sql4)
        sparkSession.sql("refresh table t_result_day_task_status")


        //从HIVE库同步任务类型统计日报至MYSQL库
        sql4 = "select * from whdb.t_result_day_task_status"
        rs_result_mid_df = sparkSession.sql(sql4)
        rs_result_mid_df.write.mode(SaveMode.Append).jdbc(url_bigdata_sys, "t_result_day_task_status", conn_info_bigdata_sys)

      }





      //按月统计任务类型
      sparkSession.sql("truncate table  whdb.t_result_mon_task_status")
      sql4 = " " +
        " insert into t_result_mon_task_status " +
        " select  substr(status_update_time,1,7), " +
        " m.task_status, " +
        " n.dic_name, " +
        " count(1) cn " +
        "  from t_imp_task m, " +
        " dic_code n " +
        "   where m.task_status=n.dic_code " +
        " and substr(status_update_time,1,7)= '" + v_mon + "'" +
        " group by substr(status_update_time,1,7), " +
        " m.task_status, " +
        " n.dic_name "

      if (v_time_type == "mon") {
        rs_result1 = sparkSession.sql(sql4)
        sql5 = s"delete from t_result_mon_task_status  where sta_time ='$v_time'"
        connection.createStatement().executeUpdate(sql5)
        //从HIVE库同步任务类型统计月报至MYSQL库
        sql4 = "select * from whdb.t_result_mon_task_status"
        rs_result_mid_df = sparkSession.sql(sql4)
        rs_result_mid_df.write.mode(SaveMode.Append).jdbc(url_bigdata_sys, "t_result_mon_task_status", conn_info_bigdata_sys)

      }



      //按周统计任务类型
      sparkSession.sql("truncate table  whdb.t_result_week_task_status")
      sql4 = " " +
        " insert into t_result_week_task_status " +
        " select  substr(status_update_time,1,4)||" + "weekofyear(status_update_time), " +
        " m.task_status, " +
        " n.dic_name, " +
        " count(1) cn " +
        "  from t_imp_task m, " +
        " dic_code n " +
        "   where m.task_status=n.dic_code " +
        " and substr(status_update_time,1,4)||" + "weekofyear(status_update_time)= '" + v_week + "'" +
        " group by substr(status_update_time,1,4)||" + "weekofyear(status_update_time), " +
        " m.task_status, " +
        " n.dic_name "

      //按周统计
      if (v_time_type == "week") {

        rs_result1 = sparkSession.sql(sql4)
        sql5 = s"delete from t_result_week_task_status  where sta_time ='$v_time'"
        connection.createStatement().executeUpdate(sql5)

        //从HIVE库同步任务类型统计周报至MYSQL库
        sql4 = "select * from whdb.t_result_week_task_status"
        rs_result_mid_df = sparkSession.sql(sql4)
        rs_result_mid_df.write.mode(SaveMode.Append).jdbc(url_bigdata_sys, "t_result_week_task_status", conn_info_bigdata_sys)

      }


      //按小时统计任务类型
      sparkSession.sql("truncate table  whdb.t_result_hour_task_status")
      sql4 = " " +
        " insert into t_result_hour_task_status " +
        " select  substr(status_update_time,1,13) , " +
        " m.task_status, " +
        " n.dic_name, " +
        " count(1) cn " +
        "  from t_imp_task m, " +
        " dic_code n " +
        "   where m.task_status=n.dic_code " +
        " and substr(status_update_time,1,13)= '" + v_hour + "'" +
        " group by substr(status_update_time,1,13), " +
        " m.task_status, " +
        " n.dic_name "

      //按小时统计
      if (v_time_type == "hour") {
        rs_result1 = sparkSession.sql(sql4)
        //先清空WEB MYSQL库中的结果数据（重复跑程序时需要删除原先已有的结果数据）
        sql5 = s"delete from t_result_hour_task_status  where sta_time ='$v_time'"
        connection.createStatement().executeUpdate(sql5)

        //从HIVE库同步任务类型统计日报至MYSQL库
        sql4 = "select * from whdb.t_result_hour_task_status"
        rs_result_mid_df = sparkSession.sql(sql4)
        rs_result_mid_df.write.mode(SaveMode.Append).jdbc(url_bigdata_sys, "t_result_hour_task_status", conn_info_bigdata_sys)
      }

      //执行完毕将周期维度的状态改为成功
      //var sql = s"select min(t.statis_time) statis_time from t_task_time_status t where t.status ='none' and t.time_type='$v_time_type'"

      //sql4 = s"update t_task_time_status t set t.status='successfull',t.info='任务状态统计成功'   where t.statis_time ='$v_time' and t.time_type='$v_time_type'"
      sql4 = s"update t_task_time_status t set t.info='任务状态统计成功'   where t.statis_time ='$v_time' and t.time_type='$v_time_type'"
      connection.createStatement().executeUpdate(sql4)


      //sparkSession.stop()
      //connection.close()

    }
    catch{

      case e: ArithmeticException => println(e)
      case ex: Throwable =>println("found a unknown exception"+ ex)
    }
    finally {

    }


  }


}
