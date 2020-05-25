/*
 * Created on 2014-12-1 9:10:10
 */
package com.demo.bigdata.utils;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * The abstract connection to provide the solution how to connect a database.
 * @author fangang
 */
public abstract class AbstractConnection implements Connection {

	/**
	 * The default constructor
	 */
	public AbstractConnection() {
		super();
	}

	/* (non-Javadoc)
	 * @see com.aisino.dzdz.jdbc.Connection#getConnection()
	 */
	public abstract java.sql.Connection getConnection();



	/**
	 * connect the database
	 * @param driver
	 * @param url
	 * @param user
	 * @param password
	 * @return the database connection
	 * @throws ClassNotFoundException when database driver not found
	 * @throws SQLException when connect the database failed
	 */
	protected java.sql.Connection doConnect(String driver, String url, String user,
			String password) throws ClassNotFoundException, SQLException {
				Class.forName(driver);
		return DriverManager.getConnection(url, user, password);
	}

}