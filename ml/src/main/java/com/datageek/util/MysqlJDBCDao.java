package com.datageek.util;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;

public class MysqlJDBCDao {
    private ResultSet res;
    private String driverName  = "";
    public String dbUrl = "";
    public String dbUser  = "";
    public String dbPassword  = "";
    //private String configtable = "";
    private Connection conn = null;
    private Statement stmt = null;

    private static Logger log = Logger.getLogger(MysqlJDBCDao.class);

    public MysqlJDBCDao() {
        try {
            PropertiesReader.loadPropertiesByClassPath("mysql.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        driverName = "com.mysql.jdbc.Driver";
        dbUrl = PropertiesReader.getProperty("db.mysql.url");
        log.info("db.mysql.url:" + dbUrl);
        dbUser = PropertiesReader.getProperty("db.mysql.user");
        log.info("db.mysql.user:" + dbUser);
        dbPassword = PropertiesReader.getProperty("db.mysql.password");
        /*configtable = PropertiesReader.getProperty("db.mysql.configtable");
        log.info("db.mysql.configtable:" + configtable);*/

        try {
            conn = getConn();
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private Connection getConn() throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    }

    public void closeConn() throws SQLException {
        if(res != null)
            res.close();
        if(stmt != null)
            stmt.close();
        if(conn != null)
            conn.close();
    }

    private int count(ResultSet rest) throws SQLException{
        int rowCount = 0;
        while(rest.next()){
            rowCount++;
        }
        rest.beforeFirst();
        return rowCount;
    }

    public void query(String sql) throws SQLException{
        res = stmt.executeQuery(sql);
        log.info("query the sql is finished, get "+count(res)+" results!");
//        while (res.next()) {
//            log.info(res.getString(1) + "\t" + res.getString(2) + "\t" + res.getString(3) + "\t" + res.getString(4));
//        }
//        res.beforeFirst();
    }
    //SET SQL_SAFE_UPDATES=0;
    public void updateAppId(String table, String condition, String value) throws SQLException{
        /*String sql = "SET SQL_SAFE_UPDATES=0";
        String sql1 = "update " + table + " set APPLICATION_ID = '" +
                value + "' where UUID = '" + condition + "'";
        log.info("========sql========"+sql + "; " + sql1);
        stmt.addBatch(sql);
        stmt.addBatch(sql1);
        //int res = stmt.executeUpdate(sql);
        int res = stmt.executeBatch()[1];
        log.info("update tables by the sql is finished! The result is " + res);*/
        String sql1 = "update " + table + " set APPLICATION_ID = '" +
                value + "' where UUID = '" + condition + "'";
        log.info("========sql========"+ sql1);
        //int res = stmt.executeUpdate(sql);
        int res = stmt.executeUpdate(sql1);
        log.info("update tables by the sql is finished! The result is " + res);
    }

    public void updateMLTrainAppId(String condition, String value) throws SQLException{
        updateAppId("ml_algorithm_main", condition, value);
    }

    public void updateMLPredictAppId(String condition, String value) throws SQLException{
        updateAppId("ml_batchalgorithm_main", condition, value);
    }

    public HashMap<String, String> getConfig(String id) throws SQLException{
        HashMap<String, String> map = new HashMap<>();
        /*String sql = "SELECT a.KEY_NAME, b.KEY_VALUE FROM bus_ml_config_detail b " +
                "JOIN bus_ml_config c ON c.CONFIG_NAME='" + type +
                "' AND b.CONFIG_ID = c.ID JOIN bus_ml_config_info a ON b.KEY_ID = a.ID";*/
        String sql = "SELECT a.KEY_NAME, b.KEY_VALUE from ml_algorithm_detail b\n" +
                "join sys_ml_algorithm_config a on b.KEY_ID = a.ID\n" +
                "join ml_algorithm_main c on b.MODEL_ID = c.id\n" +
                "and c.uuid = '"+id+"'";
        log.info("========sql========"+sql);
        res = stmt.executeQuery(sql);
        log.info("query the sql is finished, get "+count(res)+" results!");
        while (res.next()) {
            String key =  res.getString(1).toLowerCase().trim();
            String value = res.getString(2).trim();
            if(!value.equalsIgnoreCase("") && value != null){
                map.put(key, value);
            }
        }
        res.beforeFirst();
        return map;
    }

    public HashMap<String, String> getPredictConfig(String id) {
        HashMap<String, String> map = new HashMap<>();
        /*String sql = "SELECT a.KEY_NAME, b.KEY_VALUE FROM bus_ml_config_detail b " +
                "JOIN bus_ml_config c ON c.CONFIG_NAME='" + type +
                "' AND b.CONFIG_ID = c.ID JOIN bus_ml_config_info a ON b.KEY_ID = a.ID";*/
        String sql = "select select_table as sqls, " +
                "model_uuid as muuid, " +
                "output_param as dbtype, " +
                "output_table as tablename " +
                "from ml_batchalgorithm_main " +
                "where uuid = '"+id+"'";
        log.info("========sql========"+sql);
        try {
            res = stmt.executeQuery(sql);
            log.info("query the sql is finished, get " + count(res) + " results!");
            String key = "";
            String value = "";
            if (res.next()) {
                key = "sql";
                value = res.getString("sqls");
                if (value != null && !value.equalsIgnoreCase(""))
                    map.put(key, value.trim());
                key = "muuid";
                value = res.getString("muuid");
                if (value != null && !value.equalsIgnoreCase(""))
                    map.put(key, value.trim());
                key = "dbtype";
                value = res.getString("dbtype");
                if (value != null && !value.equalsIgnoreCase(""))
                    map.put(key, value.trim());
                key = "tablename";
                value = res.getString("tablename").trim();
                if (value != null && !value.equalsIgnoreCase(""))
                    map.put(key, value.trim());
            }
            res.beforeFirst();
        }catch (Exception e){
            e.printStackTrace();
        }

        return map;
    }

    public String getDbNameInDictionary() throws SQLException {
        String sql = "select dic_value from sys_dic_info where PARENT_DIC_TYPE='DAO_API' and DIC_NAME='DAO_API_SCHEMA'";
        ResultSet res = stmt.executeQuery(sql);
        if(res.next()){
            return res.getString("dic_value");
        }else{
            return "";
        }
    }

    public String getAlIdInDictionary(String muuid) throws SQLException {
        String sql = "select AL_ID from ml_algorithm_main where uuid = '"+muuid+"'";
        ResultSet res = stmt.executeQuery(sql);
        if(res.next()){
            return res.getString("AL_ID");
        }else{
            return "";
        }
    }

    public String getModelPathInDictionary(String muuid) throws SQLException {
        String sql = "select savePath from mode_save_path where jobid='"+muuid
                +"' order by timestamp desc limit 1";
        ResultSet res = stmt.executeQuery(sql);
        if(res.next()){
            return res.getString("savePath");
        }else{
            return "";
        }
    }

}
