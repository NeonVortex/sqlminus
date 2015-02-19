package com.timelessmind.smartigo.sql;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.log4j.Logger;
import com.timelessmind.smartigo.util.SQLConnMgr;
import java.util.Properties;

public class RunSQL {
    static Logger log = Logger.getLogger(RunSQL.class.getName());
    private static RunSQL instance = new RunSQL();

    static Properties properties;

    protected RunSQL() {
        try{
            properties = new Properties();
            try{
                properties.load(new FileInputStream(/*com.timelessmind.tme.util.TMEConstants.TMEJBOSSHOMEPROP+*/"tme.properties"));
            }
            catch(Exception f){
                System.out.println("Could not find tme.properties in jbosshome, looking in default");
                properties.load(new FileInputStream("tme.properties"));
            }
			
        }
        catch(Exception e){
           log.error("", e);
        }
    }

    public static RunSQL getInstance() {
        return instance;
    }

	public static void main(String args[])
    {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String url = args[0];
			String user = args[1];
			String password = args[2];
			String sql = args[3];
			boolean update = true;
			if (args.length > 4) {
				try {
					update = "1".equals(args[4]);
				}
				catch (Exception e) {
					
				}
			}
			
			conn = SQLConnMgr.getConnectionFromDriver(url, user, password);
			ps = conn.prepareStatement(sql);
			log.info("STMT = " + sql);
			log.info("update = " + update);
			if (update) {
				//20150213 ZZ only try twice
//				while (true) {
				for (int i = 0; i < 2; i++) {
					try {
						int affectedRows = ps.executeUpdate();
						log.info("affected rows = " + affectedRows);
						break;
					}
					catch (Exception e) {
						log.error("", e);
//						e.printStackTrace();
					}
				}
				try {
					conn.commit();
				}
				catch (Exception e) {
					log.error("", e);
				}
			}
			else {
				rs = ps.executeQuery();
				int columnCount = ps.getMetaData().getColumnCount();
				while (rs.next()) {
					
				}
			}
		}
		catch (Exception e) {
			log.error("", e);
		}
		finally {
			SQLConnMgr.closeResultSet(rs);
			SQLConnMgr.closeStatement(ps);
			SQLConnMgr.closeConnection(conn);
		}
		//20150219 ZZ
//		System.exit(0);
		Runtime.getRuntime().halt(0);
    }

}