/*
 * Created on 2014-12-1 9:07:44
 */
package com.demo.bigdata.utils;

/**
 * @author fangang
 */
public interface Connection {
	/**
	 * get connection of database.
	 * @return the connection
	 */
	public java.sql.Connection getConnection();
}
