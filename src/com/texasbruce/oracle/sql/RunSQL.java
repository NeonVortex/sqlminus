package com.texasbruce.oracle.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

import org.apache.log4j.Logger;

import com.texasbruce.oracle.util.SQLConnMgr;

public class RunSQL {
	private static RunSQL instance = new RunSQL();

	protected RunSQL() {
	}

	private static boolean isLogConfig = Logger.getRootLogger().getAllAppenders().hasMoreElements();
	private static Logger logger = isLogConfig ? Logger.getLogger(RunSQL.class.getName()) : null;

	public static void log(Object o) {
		log(o, null);
	}

	public static void log(Object o, Throwable e) {
		if (isLogConfig) {
			logger.info(o, e);
		} else {
			System.out.println("[" + new java.util.Date().toString() + "] " + o);
			if (e != null) {
				System.out.println(e.getMessage() + " - " + e.getClass().getName());
				for (Object st : e.getStackTrace()) {
					System.out.println("\t" + st);
				}
			}
		}
	}

	public static RunSQL getInstance() {
		return instance;
	}

	public static int runStmt(Connection conn, String stmt, boolean update, boolean logResultsOnly) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		int result = 0;
		try {
			ps = conn.prepareStatement(stmt);

			if (!logResultsOnly)
				log("STMT = " + stmt);
			if (!logResultsOnly)
				log("update = " + update);
			if (update) {
				try {
					int affectedRows = ps.executeUpdate();
					if (!logResultsOnly) {
						log("affected rows = " + affectedRows);
					}
					result = affectedRows;
				} catch (Exception e) {
					log("", e);
				}
				try {
					conn.commit();
				} catch (Exception e) {
					log("", e);
				}
			} else {
				rs = ps.executeQuery();
				int columnCount = ps.getMetaData().getColumnCount();
				int resultSize = 0;

				String output = "";
				for (int i = 1; i <= columnCount; i++) {
					output += ps.getMetaData().getColumnName(i) + "\t";
				}
				output += "\n";
				while (rs.next()) {
					for (int i = 1; i <= columnCount; i++) {
						Object o = rs.getObject(i);
						if (o instanceof oracle.sql.Datum && o.getClass().getName().contains("TIMESTAMP")) {
							o = rs.getTimestamp(i);
						} else if (o instanceof oracle.sql.Datum && o.getClass().getName().contains("CLOB")) {
							o = rs.getString(i);
						}
						output += o + "\t";
					}
					output += "\n";
					resultSize++;
				}
				log("Query result:\n" + output);
				result = resultSize;
			}
		} catch (SQLException e) {
			log("", e);
		} finally {
			SQLConnMgr.closeResultSet(rs);
			SQLConnMgr.closeStatement(ps);
		}
		return result;
	}

	public static void main(String args[]) {

		if (args.length < 3) {
			System.out.println("Usage - dbUrl dbUser dbPass [statement] [update]");
			System.out.println("\tif [statement] not provided, enter interactive loop mode.");
			System.out.println("\t[update_only] should be 1 or 0.");
			Runtime.getRuntime().halt(0);
		}

		Connection conn = null;
		try {
			String url = args[0];
			String user = args[1];
			String password = args[2];
			String sql = args.length > 3 ? args[3] : null;
			boolean update = args.length > 4 && "1".equals(args[4]);
			boolean logResultsOnly = Boolean.parseBoolean(System.getProperty("LOGRESULTSONLY", "true"));

			conn = SQLConnMgr.getConnectionFromDriver(url, user, password);

			if (sql == null || "".equalsIgnoreCase(sql)) {
				// Starting interactive shell
				try (Scanner scStdin = new Scanner(System.in)) {

					boolean toContinue = true;
					while (toContinue) {
						if (scStdin.hasNext()) {
							String stmt = scStdin.nextLine().trim();
							if (stmt == null || "".equalsIgnoreCase(stmt)) {
								toContinue = false;
							} else {
								runStmt(conn, stmt, update, logResultsOnly);
							}
						} else {
							toContinue = false;
						}
					}

				} catch (Exception e) {
					log("", e);
				}
			} else {
				runStmt(conn, sql, update, logResultsOnly);
			}
		} catch (Exception e) {
			log("", e);
		}
		SQLConnMgr.closeConnection(conn);
		// 20150219 ZZ
		// System.exit(0);
		Runtime.getRuntime().halt(0);
	}

}