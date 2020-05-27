package baicdt.bigdata.pkg

import java.sql.{Connection, DriverManager}
import java.util.Properties

class controlInben {
  //统计本月、本周、最近一周、本年累计数据接入量指标
  def runStatisInBen{
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
      var sqltext = s"select substr(now(),1,7) as dt"
      var rs1 = connection.createStatement().executeQuery(sqltext)
      while (rs1.next()) {
        v_time = rs1.getString("dt")
        if (v_time==null) {sys.exit(-1)}
      }
      //v_time="2020-05"

      //【本周及最近一周、本年、本月累计数据接入量指标】

      //本月所对应的每一天的数据接入量
      //删除上一次运行结果
      sqltext = s"delete from t_in_size_ben where dt_type='mon_day'"
      connection.createStatement().executeUpdate(sqltext)


      //统计本月每天的接入数据量 mon_day
      sqltext=" insert into  t_in_size_ben select 'mon_day', "+
        " substr(status_update_time,1,10),"+
        " sum(task_in_size)"+
        " from t_imp_task"+
        s" where substr(status_update_time,1,7)='$v_time'"+
        " group by substr(status_update_time,1,10)"

      connection.createStatement().executeUpdate(sqltext)

      //本月累计的数据接入量
      //先删除上一次运行结果
      sqltext = s"delete from t_in_size_ben where dt_type='mon_total'"
      connection.createStatement().executeUpdate(sqltext)

      //根据日报生成新的本月累计 mon_total
      sqltext=" insert into  t_in_size_ben select 'mon_total', "+
        " substr(dt,1,7),"+
        " sum((in_data_total))"+
        " from t_in_size_ben"+
        s" where substr(dt,1,7)='$v_time'"+
        " group by substr(dt,1,7)"
      connection.createStatement().executeUpdate(sqltext)




      //[本年所对应的每一天的数据接入量 start]
      //删除上一次运行结果
      v_time=v_time.substring(0,4) //获取当前年份
      sqltext = "delete from t_in_size_ben where dt_type='year_day'"
      connection.createStatement().executeUpdate(sqltext)


      //统计本年每天的接入数据量 year_day
      sqltext=" insert into  t_in_size_ben select 'year_day', "+
        " substr(status_update_time,1,10),"+
        " sum(task_in_size)"+
        " from t_imp_task"+
        s" where substr(status_update_time,1,4)='$v_time'"+
        " group by substr(status_update_time,1,10)"

      connection.createStatement().executeUpdate(sqltext)

      //本年对应的每月的数据接入量
      //先删除上一次运行结果
      sqltext = "delete from t_in_size_ben where dt_type='year_mon'"
      connection.createStatement().executeUpdate(sqltext)

      //根据当前年份中每一天的数据接入量生成每月的数据接入量 year_mon
      sqltext=" insert into  t_in_size_ben select 'year_mon', "+
        " substr(dt,1,7),"+
        " sum((in_data_total))"+
        " from t_in_size_ben"+
        s" where substr(dt,1,4)='$v_time'"+
         " and dt_type='year_day'"+
        " group by substr(dt,1,7)"
      connection.createStatement().executeUpdate(sqltext)


      //根据当前年份中每月的数据接入量求各生成当前的累计数据接入量 year_total
      //先删除上一次运行结果
      sqltext = "delete from t_in_size_ben where dt_type='year_total'"
      connection.createStatement().executeUpdate(sqltext)

      sqltext=" insert into  t_in_size_ben select 'year_total', "+
        " substr(dt,1,4),"+
        " sum((in_data_total))"+
        " from t_in_size_ben"+
        s" where substr(dt,1,4)='$v_time'"+
        " and dt_type='year_mon'"+
      " group by substr(dt,1,4)"
      connection.createStatement().executeUpdate(sqltext)
      //[本年所对应的每一天的数据接入量 end]

      //[统计最近一周的数据接入量start]

      //删除上一次运行结果
      sqltext = "delete from t_in_size_ben where dt_type='fast_week_day'"
      connection.createStatement().executeUpdate(sqltext)


      //统计最近一周每天的接入数据量 fast_week_day
      sqltext=" insert into  t_in_size_ben select 'fast_week_day', "+
        " substr(status_update_time,1,10),"+
        " sum(task_in_size)"+
        " from t_imp_task"+
        s" where substr(status_update_time,1,10)>=date_sub(curdate(),interval 7 day) "+
        " group by substr(status_update_time,1,10)"

      connection.createStatement().executeUpdate(sqltext)

      //最近一周累计的数据接入量
      //先删除上一次运行结果
      var v_cnt=0
        sqltext = "select count(1) cnt from t_in_size_ben where dt_type='fast_week_total'"
      rs1 = connection.createStatement().executeQuery(sqltext)
      while (rs1.next()) {
        v_cnt = rs1.getInt("cnt")
      }


      if (v_cnt!=0) {

        sqltext = "delete from t_in_size_ben where dt_type='fast_week_total'"
        connection.createStatement().executeUpdate(sqltext)
      } // if end
      //根据最新一周每天的接入数据量生成最近一周的累计数据接入量 fast_week_total
      sqltext=" insert into  t_in_size_ben select 'fast_week_total', "+
        " 'fast_week',"+
        " sum((in_data_total))"+
        " from t_in_size_ben"+
        " where dt_type='fast_week_day'"+
        " group by 'fast_week_total','fast_week'"

      connection.createStatement().executeUpdate(sqltext)
      //[统计最近一周的数据接入量end]




      //[统计本周的数据接入量start]
      v_time = ""  //获取当前时间所对应的在本年度的第几周
      sqltext = "select weekofyear(now()) as dt"
      rs1 = connection.createStatement().executeQuery(sqltext)
      while (rs1.next()) {
        v_time = rs1.getString("dt")
        if (v_time==null) {sys.exit(-1)}
      }


      //删除上一次运行结果(本周每天的数据接入量)
      sqltext = "delete from t_in_size_ben where dt_type='week_day'"
      connection.createStatement().executeUpdate(sqltext)


      //统计本周每天的接入数据量
      sqltext=" insert into  t_in_size_ben select 'week_day', "+
        " substr(status_update_time,1,10),"+
        " sum(task_in_size) "+
        " from t_imp_task "+
        s" where weekofyear(status_update_time)='$v_time'"+
        " group by substr(status_update_time,1,10)"

      connection.createStatement().executeUpdate(sqltext)

      //最近本周累计的数据接入量
      //先删除上一次运行结果
      v_cnt=0
      sqltext = "select count(1) cnt from t_in_size_ben where dt_type='week_total'"
      rs1 = connection.createStatement().executeQuery(sqltext)
      while (rs1.next()) {
        v_cnt = rs1.getInt("cnt")
      }


      if (v_cnt!=0) {

        sqltext = "delete from t_in_size_ben where dt_type='week_total'"
        connection.createStatement().executeUpdate(sqltext)
      }  // end if
        //根据本周每天的接入数据量生成本周的累计数据接入量
        sqltext=" insert into  t_in_size_ben select 'week_total', "+
          " 'ben_week',"+
          " sum((in_data_total))"+
          " from t_in_size_ben"+
          " where dt_type='week_day'"+
          " group by 'week_total','ben_week'"

        connection.createStatement().executeUpdate(sqltext)





      //[统计本周的数据接入量end]

      println("successfull!")
    }
    catch{

      case e: ArithmeticException => println(e)
      case ex: Throwable =>println("found a unknown exception"+ ex)
    }
    finally {
      println("finally!")
    }
  }  //end def
}
