package com.timelessmind.smartigo.util;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLConnMgr {
    static Logger logger = Logger.getLogger(SQLConnMgr.class.getName());
    private static SQLConnMgr instance = new SQLConnMgr();

    protected SQLConnMgr() {

		try {
 			DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
			//DriverManager.registerDriver(new com.mysql.jdbc.Driver());
		} catch (SQLException e) {
			logger.error("", e);
		}
    }

    public static SQLConnMgr getInstance() {
        return instance;
    }
    

	public static void closeConnection(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			}
			catch (Exception e) {
                logger.error("", e);
			}
		}
	}

	public static void closeStatement(Statement ps) {
		if (ps != null) {
			try {
				ps.close();
			}
			catch (Exception e) {
                logger.error("", e);
			}
		}
	}

	public static void closeResultSet(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			}
			catch (Exception e) {
                logger.error("", e);
			}
		}
	}

	public static java.sql.Connection getConnectionFromDriver(String location, String location_user, String location_password) {

		java.sql.Connection conn = null;
        logger.info(" getting connection... from " + location + " - " + location_user);
		for (int j = 0; j < 10; j++) {
			try {
				conn = DriverManager.getConnection(location,location_user,location_password);
				conn.setAutoCommit(false);
				break;
			}
			catch (SQLException se)
			{
                logger.error("", se);
			}
			catch (Exception ex)
			{
                logger.error("", ex);
			}
			try {
				Thread.sleep(500);
                logger.error(" Retrying to get a connection from location: " + location);
			}
			catch (Exception e) {
                logger.error("", e);
			}
		}
		return conn;
	}
	
}