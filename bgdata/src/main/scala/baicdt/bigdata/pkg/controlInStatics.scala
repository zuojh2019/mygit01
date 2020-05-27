package baicdt.bigdata.pkg
import java.sql.{Connection, DriverManager}
import java.util.Properties
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


class controlInStatics {
  //控制面版按年统计各数据源接入量占比统计
  def runControlInStatics: Unit ={

    //异常处理
    try {
      println("start")
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


      //获取mysql库中每天该处理那天的报表
      var connection: Connection = null
      classOf[com.mysql.jdbc.Driver]
      connection = DriverManager.getConnection(url_bigdata_sys, conn_info_bigdata_sys)

      //获取当前时间所对应的年份
      var v_time = ""
      var sqltext = s"select substr(now(),1,4) as dt"
      var rs1 = connection.createStatement().executeQuery(sqltext)
      while (rs1.next()) {
        v_time = rs1.getString("dt")
        if (v_time==null) {sys.exit(-1)}
      }

      //清空各数据源接入量中间表中的数据
      sqltext = s"delete from t_datasource_in_size_tmp  where dt_type='year' and dt ='$v_time'"
      connection.createStatement().executeUpdate(sqltext)


      //生成各数据源接入量中间表数据
      sqltext="insert into t_datasource_in_size_tmp " +
        "select  'year', date_format(m.status_update_time, '%Y'), " +
        "m.datasource_type,  " +
        " n.dic_name,  " +
      " sum(m.task_in_size)  " +
      " from t_imp_task m,  " +
      " dic_code n  " +
        "  where m.datasource_type=n.dic_code" +
      " and m.task_in_size <>0" +
      s" and date_format(m.status_update_time, '%Y')='$v_time'" +
      " group by date_format(m.status_update_time, '%Y')," +
      " m.datasource_type," +
      " n.dic_name "
      connection.createStatement().executeUpdate(sqltext)

      //根据中间表数据生成所有数据类型的总接入量，进而生成各数据源接入量点比
      sqltext=" select sum(task_in_size) total_task_in_size "+
              " from t_datasource_in_size_tmp m "+
              s" where m.dt='$v_time'"
       rs1 = connection.createStatement().executeQuery(sqltext)

      var v_total_task_in_size=0
      while (rs1.next()) {

        v_total_task_in_size = rs1.getInt("total_task_in_size")
        if (v_total_task_in_size==0) {sys.exit(-1)}
      }


      //清空各数据源接入量结果中间表中的数据
      sqltext = s"delete from t_datasource_in_size_result  where dt_type='year' and dt ='$v_time'"
      connection.createStatement().executeUpdate(sqltext)

      //生成各数据源接入量及占比统计结果表

      sqltext=" insert into  t_datasource_in_size_result "+
        " select 'year',m.dt, "+
      " m.datasource_type, "+
      " m.datasource_type_name, "+
      " m.task_in_size, "+
      s" $v_total_task_in_size, "+
      s" m.task_in_size/$v_total_task_in_size "+
      " from t_datasource_in_size_tmp m "+
      s" where m.dt='$v_time'"
      connection.createStatement().executeUpdate(sqltext)
      println("successfull!")
    }
    catch{

      case e: ArithmeticException => println(e)
      case ex: Throwable =>println("found a unknown exception"+ ex)
    }
    finally {
      println("successfull two!")
    }

  } //def runControlInStatics end

   //控制面版总接入量、总任务数及总失败任务数统计
  def runControlInStatics_total: Unit ={

    //异常处理
    try {
      println("start")
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


      //获取mysql库中每天该处理那天的报表
      var connection: Connection = null
      classOf[com.mysql.jdbc.Driver]
      connection = DriverManager.getConnection(url_bigdata_sys, conn_info_bigdata_sys)


      //获取当前时间所对应的年份
      var v_time = ""
      var sqltext = s"select substr(now(),1,10) as dt"
      var rs1 = connection.createStatement().executeQuery(sqltext)
      while (rs1.next()) {
        v_time = rs1.getString("dt")
        if (v_time==null) {sys.exit(-1)}
      }
      //v_time="2020-05-13"

      //计算总接入数据量、总任务数、总失败任务数
      //总接入数据量
      var v_task_in_total_size=0
      //总任务数
      var v_task_cnt=0
      //总失败任务数
      var v_fail_task_cnt=0
      //总成功任务数
      var v_in_ok_task_total=0

      //task_status  2是已完成 3是失败
      sqltext="  select sum(task_in_size) task_in_total_size,count(1) task_cnt, sum(case when task_status='3' then 1 else 0 end) fail_task_cnt,"+
       " sum(case when task_status='2' then 1 else 0 end) in_ok_task_total  " +
       " from t_imp_task "

      rs1 = connection.createStatement().executeQuery(sqltext)

      while (rs1.next()) {
        v_task_in_total_size = rs1.getInt("task_in_total_size")
        v_task_cnt=rs1.getInt("task_cnt")
        v_fail_task_cnt=rs1.getInt("fail_task_cnt")
        v_in_ok_task_total=rs1.getInt("in_ok_task_total")

        if (v_task_cnt==0) {sys.exit(-1)}
      }


      //清空累计接入总表的数据
      sqltext = "delete from t_in_size_total "
      connection.createStatement().executeUpdate(sqltext)


      //生成各数据源接入量中间表数据
      sqltext="insert into t_in_size_total " +
           s"values($v_task_in_total_size"+
            s",$v_task_cnt"+
            s",$v_fail_task_cnt"+
            s",$v_in_ok_task_total)"
      connection.createStatement().executeUpdate(sqltext)



      //生成今日接入数据总量、今日接入任务总数、今日接入成功任务数

      //先删除今日和今日对应的每个小时的结果数据
      sqltext=s"delete from t_in_size_today  where substr(dt,1,10)='$v_time'"
      connection.createStatement().executeUpdate(sqltext)

      //每次运行生成今日和今日对应的每个小时的结果数据


      //生成今日每小时所对应的统计数
      sqltext=" insert into t_in_size_today "+
        " select 'hour', "+
        " concat(substr(status_update_time,1,13),':00:00'), "+
        " sum(task_in_size) task_in_size, "+
        " count(1) task_cnt, "+
        " sum(case when task_status='2' then 1 else 0 end)  task_ok_cnt,"+
      " sum(case when task_status='3' then 1 else 0 end) task_fail_cnt"+
      s" from t_imp_task where substr(status_update_time,1,10)='$v_time' "+
      " group by  concat(substr(status_update_time,1,13),':00:00')"
      connection.createStatement().executeUpdate(sqltext)

      //根据今日每小时统计数求和算今日总数

      sqltext=" insert into t_in_size_today "+
        " select 'today', "+
        " substr(dt,1,10), "+
        " sum(in_data_total) , "+
        " sum(in_task_total), "+
        " sum(in_ok_task_total),"+
        " sum(in_fail_task_total)"+
        s" from t_in_size_today where substr(dt,1,10)='$v_time' "+
        " and dt_type='hour'"+
        " group by  substr(dt,1,10)"
      connection.createStatement().executeUpdate(sqltext)

      println("successfull!")
    }
    catch{

      case e: ArithmeticException => println(e)
      case ex: Throwable =>println("found a unknown exception"+ ex)
    }
    finally {
      println("finally!")
    }

  }  // def runControlInStatics_total  end



}  //class end
