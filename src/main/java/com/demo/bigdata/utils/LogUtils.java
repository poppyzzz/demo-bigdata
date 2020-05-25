/*
 * created on 2016.5.7 
 */
package com.demo.bigdata.utils;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 写入日志表工具
 * @author fangang
 */
public class LogUtils extends AbstractConnection implements Connection {
	private static java.sql.Connection connection= null;
	@Override
	public java.sql.Connection getConnection() {
		if(connection==null){
			String driver = PropertyFile.getProperty("driver");
			String url = PropertyFile.getProperty("url");
			String user = PropertyFile.getProperty("user");
			String pwd = PropertyFile.getProperty("pwd");
			
			try {
				return doConnect(driver, url, user, pwd);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("The driver not found when connect the oracle database", e);
			} catch (SQLException e) {
				throw new RuntimeException("error when connect the oracle database", e);
			}
		}
		return connection;
	}
	
	/**
	 * write a log when start a new task
	 * @param task
	 * @return id
	 */
	public static String start(String task){
		System.out.println("###############"+"start:" + task);
		//获取开始时间
		String id = task+"_"+DateUtils.getNow();
		
		//数据库连接
		if(connection==null) {
			connection = (new LogUtils()).getConnection();
		}
		//插入新数据
		String sql = "INSERT INTO dw_log (id,task,start_time) VALUES (?,?,cast(to_timestamp(clock_timestamp(),'yyyy-mm-dd hh24:mi:SS') as timestamp without time zone))";
		try {
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1,id);
			ps.setString(2,task);
			ps.executeUpdate();  
			ps.close();  
		} catch (SQLException e) {
			throw new RuntimeException("Error when insert log: [id:"+id+"]",e);
		}	
		return id;
	}
	
	/**
	 * @param id
	 * @return isSuccess
	 */
	public static void end(String id){
		//数据库连接
		if(connection==null) {
			connection = (new LogUtils()).getConnection();
		}
		//通过id查询数据，如果存在插入结束时间
		String sql2 = "UPDATE dw_log SET "
				+ "end_time = cast(to_timestamp(clock_timestamp(),'yyyy-mm-dd hh24:mi:SS') as timestamp without time zone),"
				+ "during = cast(to_timestamp(clock_timestamp(),'yyyy-mm-dd hh24:mi:SS') as timestamp without time zone)-start_time,"+
				"is_success = 'Y' WHERE id = ?"; 
		try {
			
			PreparedStatement ps2 = connection.prepareStatement(sql2);
            ps2.setString(1,id);
            ps2.executeUpdate();
            ps2.close();
		} catch (SQLException e) {
			throw new RuntimeException("Error when update log: [id:"+id+"]",e);
		}
	}
	
	/**
	 * @param id
	 * @param ex
	 */
	public static void error(String id, Exception ex){
		String message = ex.toString()+"\n";
		if(ex.getCause()!=null)
			message += "Caused by: "+ ex.getCause().getMessage();
		if(message.length()>4000) message = message.substring(0, 4000);

		//数据库连接
		if(connection==null) {
			connection = (new LogUtils()).getConnection();
		}
		//通过id查询数据，如果存在插入结束时间
		String sql2 = "UPDATE dw_log SET "
				+ "end_time = cast(to_timestamp(clock_timestamp(),'yyyy-mm-dd hh24:mi:SS') as timestamp without time zone), "
				+ "during = cast(to_timestamp(clock_timestamp(),'yyyy-mm-dd hh24:mi:SS') as timestamp without time zone)-start_time,"+
				"is_success = 'N', error_msg = ?  WHERE id = ?"; 
		try {
			
			PreparedStatement ps2 = connection.prepareStatement(sql2);
			ps2.setString(1,message);
            ps2.setString(2,id);
            ps2.executeUpdate();
            ps2.close();
		} catch (SQLException e) {
			throw new RuntimeException("Error when update log: [id:"+id+"]",e);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		LogUtils.start("task");
		String task1 = LogUtils.start("task1");
		LogUtils.end(task1);
		String task2 = LogUtils.start("task2");
		LogUtils.error(task2, new Exception("A new Error"));
	}

}
