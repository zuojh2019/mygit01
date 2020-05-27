package baicdt.bigdata.pkg

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import baicdt.bigdata.pkg.statisTaskStatus
import baicdt.bigdata.pkg.statisTaskTypeResult
import baicdt.bigdata.pkg.controlInStatics
import baicdt.bigdata.pkg.controlInben



/**
 * User: junhua zuo
 * Date: 2020/5/11 9:07
 * Description:
 */
object SparkSQLApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    System.setProperty("user.name", "hdfs")


    //本周及最近一周、本年、本月累计数据接入量指标
    var ControlInben:controlInben= new controlInben
    ControlInben.runStatisInBen

    //控制页面，任务数统计
    var ControlInStatics:controlInStatics=new controlInStatics

    //统计累计接入数据总量、总任务数、总失败任务数
    ControlInStatics.runControlInStatics_total

    //按年统计各数据源接入数据量占比
    ControlInStatics.runControlInStatics



    if (args.length==4) {
      println(args(0).toString)
      println(args(1).toString)
      println(args(2).toString)
      println(args(3).toString)
    }
    if (args.length !=1) {
      println("请输入统计维度参数 日 day 月 mon 周 week 时 hour")
      sys.exit(-1)
    }
    if (args.length==1) {
      //var ary_timetype=args[0].toString()
      //按天统计
      var ary_timetype = "day"
      ary_timetype=args(0).toString

      var stStatus: statisTaskStatusResult = new statisTaskStatusResult
      stStatus.runStatisTaskStatus(ary_timetype)

      println("ccc")
      var stType:statisTaskTypeResult=new statisTaskTypeResult
      stType.runStatisTaskType(ary_timetype)

      println("ddd")
    }




    //System.exit(0)
    //System.exit()

/* start
    //try{
     var v_date =""

      val sparkSession: SparkSession = SparkSession.builder()
      .appName("spark-sql-demo")
      //.master("local[*]")
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







    ////任务类型统计模块////



    //获取mysql的连接
    //val url = "jdbc:mysql://localhost:3306/web01?characterEncoding=utf-8"
    val url2 = "jdbc:mysql://ambari03.baicdt.com:3306/bitestdb"
    //val driver = "com.mysql.jdbc.Driver"
    //数据入库，需要new一个Properties方法
    val conn_info2 = new Properties()

    //获取数据库的用户名，密码和运行的driver类
    conn_info2.setProperty("user", "root")
    conn_info2.setProperty("password", "123456")
    //conn_info2.setProperty("user", "test")
    //conn_info2.setProperty("password", "test")
    conn_info2.setProperty("driver", "com.mysql.jdbc.Driver")

    //统计那天数据的变量

    //获取mysql库中每天该处理那天的报表
    var connection: Connection = null
    classOf[com.mysql.jdbc.Driver]
    connection = DriverManager.getConnection(url2, conn_info2)
    //val sql = s"update test_data.mysql_stu_info set age = $age where name='$name'"
    var sql = "select min(t.f_day) f_day from t_schedule_status t where t.status ='none' "


    try {
      //在spark中如果不写会出错

      var   resultSet1= connection.createStatement().executeQuery(sql)
      while (resultSet1.next()) {
        println(resultSet1.getObject(1))
          println(resultSet1.getString(1))
          println(resultSet1.getDate("f_day"))

        println(resultSet1.getString("f_day"))
        println(resultSet1.getMetaData.getColumnCount)
        println(resultSet1.getMetaData.getColumnName(1))
        println(resultSet1.getMetaData.getColumnType(1))
        println(resultSet1.getMetaData.getColumnLabel(1))
        //获取统计那天的报表
        v_date=resultSet1.getString("f_day")
      }
    } catch {
      case e: Exception=> e.printStackTrace()
    } finally {

      //intln(resultSet1.getString("f_day"))
      //connection.close()
    }





    /*
      //修改验证

    sql = "update t1 set name='ccc' where id=1"
    connection.createStatement().executeUpdate(sql)
    println(s"=====update succeed !======")

    sql = "delete from t1 where id=2"
    connection.createStatement().executeUpdate(sql)
    println(s"=====insert  succeed !======")

    sql = "insert into t1 values(4,'ddd')"
    connection.createStatement().executeUpdate(sql)
    println(s"=====insert  succeed !======")
    */

    //sql = "insert into t1 select a.* from t1 a,t1 b where a.id=b.id and a.id=4"
    //connection.createStatement().executeUpdate(sql)
    //println(s"=====insert  succeed !======")
    //t_schedule_status

   /*
    // 按分区插入到mysql中
    insertDF.foreachPartition(iter => {
      try {
        classOf[com.mysql.jdbc.Driver]
        connection = DriverManager.getConnection(url, props)
        while (iter.hasNext) {
          val row = iter.next()
          val name = row.getAs[String]("name")
          val age = row.getAs[Int]("age")
          val sql = s"insert into test_data.mysql_stu_info(name,age) values('$name',$age)"
          connection.createStatement().executeUpdate(sql)
          println(s"=====$name insert succeed !======")
        }
      } catch {
        case e: Exception => println(e.printStackTrace())
      } finally {
        connection.close()
      }
    })

    */





    //客户日增量
    var sqlText1="select "+
      "cif_type,"+
      "cif_type_name,"+
      "account_flag,"+
      "active_flag,"+
      "area_code,"+
      "date_format(reg_time,'yyyyMMdd') as tj_day,"+
      "count(1) as cnt "+
      "from t_user_reg "+
      "where reg_time=to_date(' " +v_date +  " ', 'yyyy-MM-dd') "+
      "group by cif_type, "+
      "cif_type_name,"+
      "account_flag,"+
      "active_flag,"+
      "area_code,"+
      "date_format(reg_time,'yyyyMMdd') "


    val rs_cif_day_DF: DataFrame = sparkSession.sql(sqlText1)
    rs_cif_day_DF.createOrReplaceTempView("v_mid_cif_day_cnt")
    //rs_cif_day_DF.show()
    /*
    rs_cif_day_DF.foreach(iter=>{
      if iter.hasNext then
         var row=iter.Next
          row.get
      else





    })
   */






    //客户数累计（适用于日 月 年报）
    var sqlText2=" select "+
      "cif_type, "+
      "cif_type_name, "+
      "account_flag, "+
      "active_flag, "+
      "area_code, "+
      "count(1)  as sum_cnt "+
      "from t_user_reg "+
      "where reg_time<=to_date(' " +v_date +  " ', 'yyyy-MM-dd') "+
      "group by cif_type, "+
      "cif_type_name, "+
      "account_flag, "+
      "active_flag, "+
      "area_code "

    val rs_cif_day_sum_DF: DataFrame = sparkSession.sql(sqlText2)

    rs_cif_day_sum_DF.createOrReplaceTempView("v_mid_cif_day_sum_cnt")
    //rs_cif_day_sum_DF.show()


   //日增量表与累计表关联生成日报
    sqlText2="insert into whdb.t_day_cif_cnt_total "+
      "select m.cif_type, "+
      "m.cif_type_name, "+
      "m.account_flag, "+
      "m.active_flag, "+
      "m.cnt, "+       //日新增客户数
      "n.sum_cnt, "+   //累计客户数
      "m.tj_day, "+
      "m.area_code, "+
      "now() "+
      "from v_mid_cif_day_cnt m, "+
      "     v_mid_cif_day_sum_cnt n "+
      "where m.cif_type=n.cif_type "+
      "and m.cif_type_name=n.cif_type_name "+
      "and m.account_flag=n.account_flag "+
      "and m.active_flag=n.active_flag "+
      "and m.area_code=n.area_code "

    val rs_t_day_cif_cnt_total_df =sparkSession.sql(sqlText2)
    //rs_t_day_cif_cnt_total_df.show()


    //获取mysql的连接
    //val url = "jdbc:mysql://localhost:3306/web01?characterEncoding=utf-8"
    val url1 = "jdbc:mysql://ambari03.baicdt.com:3306/bitestdb"
    //val driver = "com.mysql.jdbc.Driver"
    //数据入库，需要new一个Properties方法
    val conn_info1 = new Properties()

    //获取数据库的用户名，密码和运行的driver类
    conn_info1.setProperty("user", "root")
    conn_info1.setProperty("password", "123456")
    //conn_info.setProperty("user", "test")
    //conn_info.setProperty("password", "test")
    conn_info1.setProperty("driver", "com.mysql.jdbc.Driver")
    //将spark-sql生成的报表结果表同步至BI报表库。
    //同步日报
    //rs_t_day_cif_cnt_total_df1.write.mode(SaveMode.Append).jdbc(url,"t_day_cif_cnt_total",conn_info)





    //客户月增量
    sqlText1="select "+
      "cif_type,"+
      "cif_type_name,"+
      "account_flag,"+
      "active_flag,"+
      "area_code,"+
      "date_format(reg_time,'yyyyMM') as tj_mon,"+
      "count(1) as cnt "+
      "from t_user_reg "+
      "where trunc(reg_time,'mm') = trunc('" +v_date +  "','mm') " +
      //"where reg_time<=to_date(' " +v_date +  " ', 'yyyy-MM-dd') "+
      //" and reg_time>=to_date(trunc('" +v_date +  "','mm'),'yyyy-MM-dd') "+
      "group by cif_type, "+
      "cif_type_name,"+
      "account_flag,"+
      "active_flag,"+
      "area_code,"+
      "date_format(reg_time,'yyyyMM') "

    val rs_cif_mon_DF: DataFrame = sparkSession.sql(sqlText1)
    rs_cif_mon_DF.createOrReplaceTempView("v_mid_cif_mon_cnt")
    rs_cif_mon_DF.show()

    //月增量表与累计表关联生成月报

    sqlText2="insert into whdb.t_mon_cif_cnt_total "+
      "select m.cif_type, "+
      "m.cif_type_name, "+
      "m.account_flag, "+
      "m.active_flag, "+
      "m.cnt, "+       //日新增客户数
      "n.sum_cnt, "+   //累计客户数
      "m.tj_mon, "+
      "m.area_code, "+
      "now() "+
      "from v_mid_cif_mon_cnt m, "+
      "     v_mid_cif_day_sum_cnt n "+
      "where m.cif_type=n.cif_type "+
      "and m.cif_type_name=n.cif_type_name "+
      "and m.account_flag=n.account_flag "+
      "and m.active_flag=n.active_flag "+
      "and m.area_code=n.area_code "

    val rs_t_mon_cif_cnt_total_df =sparkSession.sql(sqlText2)
    //rs_t_mon_cif_cnt_total_df.show()




    //客户年增量
    sqlText1="select "+
      "cif_type,"+
      "cif_type_name,"+
      "account_flag,"+
      "active_flag,"+
      "area_code,"+
      "date_format(reg_time,'yyyy') as tj_year,"+
      "count(1) as cnt "+
      "from t_user_reg "+
      "where trunc(reg_time,'yyyy') = trunc('" +v_date +  "','yyyy') " +
      //"where reg_time<=to_date('" +v_date +  "', 'yyyy-MM-dd') "+
      //" and reg_time>=to_date(trunc('" +v_date +  "','yyyy'),'yyyy-MM-dd') "+
      "group by cif_type, "+
      "cif_type_name,"+
      "account_flag,"+
      "active_flag,"+
      "area_code,"+
      "date_format(reg_time,'yyyy') "

    val rs_cif_year_DF: DataFrame = sparkSession.sql(sqlText1)
    rs_cif_year_DF.createOrReplaceTempView("v_mid_cif_year_cnt")
    //rs_cif_year_DF.show()


    //年增量表与累计表关联生成年报
    sqlText2="insert into whdb.t_year_cif_cnt_total "+
      "select m.cif_type, "+
      "m.cif_type_name, "+
      "m.account_flag, "+
      "m.active_flag, "+
      "m.cnt, "+       //日新增客户数
      "n.sum_cnt, "+   //累计客户数
      "m.tj_year, "+
      "m.area_code, "+
      "now() "+
      "from v_mid_cif_year_cnt m, "+
      "     v_mid_cif_day_sum_cnt n "+
      "where m.cif_type=n.cif_type "+
      "and m.cif_type_name=n.cif_type_name "+
      "and m.account_flag=n.account_flag "+
      "and m.active_flag=n.active_flag "+
      "and m.area_code=n.area_code "

    val rs_t_year_cif_cnt_total_df =sparkSession.sql(sqlText2)
    //rs_t_year_cif_cnt_total_df.show()



    //获取mysql的连接
    //val url = "jdbc:mysql://localhost:3306/web01?characterEncoding=utf-8"
    val url = "jdbc:mysql://ambari03.baicdt.com:3306/bitestdb"
    //val driver = "com.mysql.jdbc.Driver"
    //数据入库，需要new一个Properties方法
    val conn_info = new Properties()

    //获取数据库的用户名，密码和运行的driver类
      conn_info.setProperty("user", "root")
      conn_info.setProperty("password", "123456")
      //conn_info.setProperty("user", "test")
      //conn_info.setProperty("password", "test")
    conn_info.setProperty("driver", "com.mysql.jdbc.Driver")
    //将spark-sql生成的报表结果表同步至BI报表库。
    //同步日报
    sqlText2="select * from whdb.t_day_cif_cnt_total"
    val rs_t_day_cif_cnt_total_write_df=sparkSession.sql(sqlText2)
    rs_t_day_cif_cnt_total_write_df.write.mode(SaveMode.Append).jdbc(url,"t_day_cif_cnt_total",conn_info)
    //同步月报
    sqlText2="select * from whdb.t_mon_cif_cnt_total"
    val rs_t_mon_cif_cnt_total_write_df=sparkSession.sql(sqlText2)
    rs_t_mon_cif_cnt_total_write_df.write.mode(SaveMode.Append).jdbc(url,"t_mon_cif_cnt_total",conn_info)
    //rs_t_mon_cif_cnt_total_df
    //同步年报
    sqlText2="select * from whdb.t_year_cif_cnt_total"
    val rs_t_year_cif_cnt_total_write_df=sparkSession.sql(sqlText2)
    rs_t_year_cif_cnt_total_write_df.write.mode(SaveMode.Append).jdbc(url,"t_year_cif_cnt_total",conn_info)

    //统计完成，将统计那天的状态改为统计成功
    sql = "update  t_schedule_status t set t.status='successful'  where t.f_day ='"+v_date+"'"
    connection.createStatement().executeUpdate(sql)
    //connection.createStatement().executeQuery(sql)
    connection.close()
    sparkSession.close()

















/**

    val rsDF: DataFrame = sparkSession.sql("select * from myhive.student")
    rsDF.show()
    val rsDF1: DataFrame = sparkSession.sql("select * from myhive.t3")
    rsDF1.show()

    val rsDF2: DataFrame = sparkSession.sql("insert into  myhive.t3 values(12)")
    rsDF2.show()
    val rsDF3: DataFrame = sparkSession.sql("select * from myhive.t3")
    rsDF3.show()

    //获取mysql的连接
    //val url = "jdbc:mysql://localhost:3306/web01?characterEncoding=utf-8"
    val url = "jdbc:mysql://ambari03.baicdt.com:3306/cboard"
    val driver = "com.mysql.jdbc.Driver"
    //数据入库，需要new一个Properties方法
    val conn_info = new Properties()

    //获取数据库的用户名，密码和运行的driver类
    conn_info.setProperty("user", "root")
    conn_info.setProperty("password", "123456")
    conn_info.setProperty("driver", "com.mysql.jdbc.Driver")


    //read2.write.mode(SaveMode.Append).jdbc(url, "emp", conn)
    val tab_name="t3"
    rsDF3.write.mode(SaveMode.Append).jdbc(url,tab_name,conn_info)
    rsDF3.write.mode(SaveMode.Overwrite).jdbc(url,tab_name,conn_info)

    //（第二种）方式读取数据库中数据
    //val rsDF33 = spark.read.format("jdbc").jdbc(url, tab_name, conn_info)
    //rsDF33.show()


    val rsDF4: DataFrame = sparkSession.sql("select id,count(1) as cnt from myhive.t3 group by id order by cnt")
    rsDF4.show()

    val rsDF5: DataFrame = sparkSession.sql("select id,count(1) as cnt from myhive.t3 group by id order by cnt desc")
    rsDF5.show()
    val rsDF6: DataFrame = sparkSession.sql("select * from myhive.t3 where id=12")
    rsDF6.show()


    //（第二种）方式读取数据库中数据
    val rsDF33: DataFrame = sparkSession.read.format("jdbc").jdbc(url, tab_name, conn_info)
    rsDF33.show()
    val rsDF44: DataFrame = sparkSession.read.format("jdbc").jdbc(url, "t4", conn_info)
    //rsDF44.createOrReplaceTempView("tmp_t4")
    rsDF44.createOrReplaceTempView("tmp_t4")
    rsDF44.show()
    val rsDF44tmp: DataFrame = sparkSession.sql("select * from tmp_t4 m join myhive.tmp_t4 n on m.id=n.id")
    rsDF44tmp.show()
    println("mysql的表与hive库中的表join成功！")
    val rsDF44tmphive: DataFrame = sparkSession.sql("insert into myhive.tmp_t4_insert  select * from tmp_t4")
    rsDF44tmphive.show()

    val rsDF55tmphive: DataFrame = sparkSession.sql("select * from myhive.tmp_t4_insert")
    rsDF55tmphive.show()

    rsDF44.write.mode(SaveMode.Append).jdbc(url, "t5", conn_info)
    val rsDF55: DataFrame = sparkSession.read.format("jdbc").jdbc(url, "t5", conn_info)

    /*
        ////将ES的表读取到SPARK-SQL
        val options = Map(
          "es.nodes.wan.only" -> "true",
          "es.nodes" -> "29.29.29.29:10008,29.29.29.29:10009",
          "es.batch.size.bytes"->"102400",
          "es.batch.write.reflesh"->"true",
          "es.port" -> "9200",
          "es.read.field.as.array.include" -> "arr1, arr2"
        )
        //spark.read.format("es").option(options).load("index1/info")

        //spark.read.format("es").Option(options).load("index1/info")

        val esdf1=spark.read.format("es").options(options).load("index1/info")
        esdf1.show()
        ////将ES的表读取到SPARK-SQL

        ////[将SPARK-SQL中的表写入到ES]
        val options1 = Map(
          "es.index.auto.create" -> "true",
          "es.nodes.wan.only" -> "true",
          "es.nodes" -> "29.29.29.29:10008,29.29.29.29:10009",
          "es.port" -> "9200",
          "es.mapping.id" -> "id",
          "es.batch.size.bytes" -> "102400",  //批量写入ES
          "es.batch.write.reflesh" -> "true"  //SPARK-SQL数据写入ES后刷新ES中的索引
        )

        val sourceDF = spark.table("hive_table")
        sourceDF
          .write
          .format("org.elasticsearch.spark.sql")
          .options(options)
          .mode(SaveMode.Append)
          .save("hive_table/docs")

        spark.sql("select * from t5").write.format("org.elasticsearch.spark.sql").options(options1).mode(SaveMode.Overwrite).save("t5/docs")
        ////[将SPARK-SQL中的表写入到ES]


        //spark-sql读取ES中数据的第二种方式
        val options = Map(
          "pushdown" -> "true",
          "es.nodes.wan.only" -> "true",
          "es.nodes" -> "29.29.29.29:10008,29.29.29.29:10009",
          "es.port" -> "9200"
        )

        val df = spark.esDF("hive_table/docs", "?q=user_group_id:3", options)
        df.show()
        //spark-sql读取ES中数据的第二种方式

        ////[将SPARK-SQL中的表写入到ES]的第二种方式
        val brandDF = sparkSession.sql(""" SELECT
                                         |   categoryname AS id
                                         | , concat_ws(',', collect_set(targetword)) AS targetWords
                                         | , get_utc_time() as `@timestamp`
                                         | FROM  t1
                                         | GROUP BY
                                         | categoryname
                  """.stripMargin)

        // 手动指定ES _id值
        val map = Map("es.mapping.id" -> "id")
        EsSparkSQL.saveToEs(brandDF, "mkt_noresult_brand/mkt_noresult_brand", map)


        ////[将SPARK-SQL中的表写入到ES]的第二种方式



        //rsDF.write.csv("hdfs://ambari01.baicdt.com:8020/test/spark/output05")
        //sc.stop()
        spark.stop()

















        /**

        //获取spark的连接
        val session = SparkSession.builder()
          .master("local")
          .appName(JDBCDemo.getClass.getSimpleName)
          .getOrCreate()
        import session.implicits._
        //获取mysql的连接
        val url = "jdbc:mysql://localhost:3306/web01?characterEncoding=utf-8"

        val tname = "v_ip"
        val driver = "com.mysql.jdbc.Driver"
        //（第一种）方式：从mysql中读取数据，read.format方法，最后必须用load来执行
        val load = session.read.format("jdbc").options(
          Map("url" -> url,
            "dbtable" -> tname,
            "user" -> "root",
            "password" -> "root",
            "driver" -> driver
          )
        ).load()
        //查看表结构
        load.printSchema()
        //输出表的数据类型
        println(load.schema)

        //查询表中cnts > 100 数据
        val read: Dataset[Row] = load.where("cnts > 100")
        //展示的内容用到show方法
        //        .show()

        //数据入库，需要new一个Properties方法
        val conn = new Properties()

        //获取数据库的用户名，密码和运行的driver类
        conn.setProperty("user", "root")
        conn.setProperty("password", "root")
        conn.setProperty("driver", driver)

        //（第二种）方式读取数据库中数据
        val read2 = session.read.format("jdbc").jdbc(url, tname, conn)
        read2.show()

        //（第三种）方式读取数据库中内容
        val read3 = session.read.jdbc(url,tname,conn)

        //写入数据库的（第一种）方法(此方法是默认模式（存在该表就直接报错）)
        //调用jdbc方法，方法里面的参数第一个是定义的url数据库连接，第二个是表名，第三个是Properties类的实例化对象（我们命名为conn）
        read.write.jdbc(url, "emp", conn)

        //写入数据库的（第二种）方法：调用mode方法并传入 SaveMode.Append 参数  （就是存在该表的情况下就直接在表后面追加）
        read2.write.mode(SaveMode.Append).jdbc(url, "emp", conn)

        //写入数据库（第三种）方式，调用mode方法并传入 SaveMode.Overwrite 参数 (吐过存在该表的情况下 覆盖里面的数据)
        read3.write.mode(SaveMode.Overwrite).jdbc(url, "emp", conn)

        session.close()

         **/

    */


*/

/*

object mysqlApp {
  def main(args: Array[String]): Unit = {
    val username = "root"
    val password = "1234"
    val drive = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/mysql"
    var connection: Connection = null
    try {
      //在spark中如果不写会出错
      classOf[com.mysql.jdbc.Driver]
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select user from user")
      while (resultSet.next()) {
        println(resultSet.getString("user"))
      }
    } catch {
      case e: Exception=> e.printStackTrace()
    } finally {
      connection.close()
    }
  }
}

*/

start */



  }
}
