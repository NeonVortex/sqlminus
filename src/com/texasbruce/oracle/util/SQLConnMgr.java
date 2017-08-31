package com.texasbruce.oracle.util;

import com.texasbruce.oracle.sql.RunSQL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLConnMgr {
    private static SQLConnMgr instance = new SQLConnMgr();

    protected SQLConnMgr() {

		try {
 			DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
			//DriverManager.registerDriver(new com.mysql.jdbc.Driver());
		} catch (SQLException e) {
			RunSQL.log("", e);
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
                RunSQL.log("", e);
			}
		}
	}

	public static void closeStatement(Statement ps) {
		if (ps != null) {
			try {
				ps.close();
			}
			catch (Exception e) {
                RunSQL.log("", e);
			}
		}
	}

	public static void closeResultSet(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			}
			catch (Exception e) {
                RunSQL.log("", e);
			}
		}
	}

	public static java.sql.Connection getConnectionFromDriver(String location, String location_user, String location_password) {

		java.sql.Connection conn = null;
        RunSQL.log(" getting connection... from " + location + " - " + location_user);
		for (int j = 0; j < 10; j++) {
			try {
				conn = DriverManager.getConnection(location,location_user,location_password);
				conn.setAutoCommit(false);
				break;
			}
			catch (SQLException se)
			{
                RunSQL.log("", se);
			}
			catch (Exception ex)
			{
                RunSQL.log("", ex);
			}
			try {
				Thread.sleep(500);
                RunSQL.log(" Retrying to get a connection from location: " + location);
			}
			catch (Exception e) {
                RunSQL.log("", e);
			}
		}
		return conn;
	}
	
}