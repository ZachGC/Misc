/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.momentum.multiply.SQL;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;

/**
 * Intended to be used in connecting to databases.<br>
 * Current SQL types supported: AS400|DB2|SQL Server. <br>
 * To be updated: MySQL|Derby. This program is coded on the current JDK
 * (8u51).<br>
 * <br>
 * Contact zachary.christophers@gmail.com for further updates. <br>
 * Please acknowledge the source of your code when using it.<br>
 *
 * @author Zachary Christophers
 * @version 1.0.1
 */
public class DBConnector {

    private String driver, url, username, password;
    private Connection conn = null;
    private boolean connectionSuccessful;

    public boolean isConnectionSuccessful() {
        return connectionSuccessful;
    }

    public Connection getConnection() {
        return conn;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Connection getConn() {
        return conn;
    }

    public void setConn(Connection conn) {
        this.conn = conn;
    }

    /**
     * Constructor. Populates protected variables
     */
    public DBConnector() {
        this(null, null, null, null);
    }

    public DBConnector(String driver, String url, String username, String password) {
        try {
            this.driver = driver;
            this.url = url;
            this.username = username;
            this.password = password;
            conn = createConnection();
        } catch (Exception e) {
            System.out.println("The specified database is not in the records. Please contact the administrator of this program to add it. \nPlease include all relevant information, including the SQL language being used and the relevant URL.");
        }
    }

    public DBConnector(String dbtype) {
        this(dbtype, null, null);
    }

    public DBConnector(String dbtype, String username, String password) {
        String[] drivers = {
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "com.ibm.as400.access.AS400JDBCDriver",
            "com.ibm.db2.jcc.DB2Driver"
        };
        String[] urls = {
            "jdbc:sqlserver://mmdkfvuhtdev01\\MSSQLSERVER:1433;databaseName=ZACH",
            "jdbc:as400://isd.momentum.co.za:1433/lpprdlib",
            "jdbc:db2://EDB2DEV3:60022/MMULTDCS"
        };
        try {
            boolean found = false;
            for (int i = 0; i < 3; i++) {
                if (drivers[i].contains(dbtype.toLowerCase())) {
                    driver = drivers[i];
                    url = urls[i];
                    found = true;
                }
            }
            if (found) {
                this.username = username;
                this.password = password;
                conn = createConnection();
            } else {
                throw new Exception();
            }
        } catch (Exception e) {
            System.out.println("The specified database is not in the records. Please contact the administrator of this program to add it. \nPlease include all relevant information, including the SQL language being used and the relevant URL.");
        }
    }

    /**
     * Creates a connection to the database specified in the constructor.
     *
     * @return Returns the connection created to the database.
     * @throws ClassNotFoundException In the event that the driver is not found.
     * @throws SQLException In the event that a connection cannot be made to the
     * database
     */
    public Connection createConnection() {
        Connection conn = null;
        try {
            Class.forName(driver);

            //STEP 3: Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(url, username, password);
            System.out.println("Connection successful.");
            connectionSuccessful = true;
        } catch (ClassNotFoundException ex) {
            System.out.println("Class " + driver + " not found. At com.momentum.multiply.SQl.DBConnector.createConnection (line 150)");
            Logger.getLogger(DBAccess.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
            connectionSuccessful = false;
        } catch (SQLException ex) {
            System.out.println("Failed to connect to database at URL " + url
                    + " with username " + username
                    + " and password " + password + "."
                    + "\nPlease see com.momentum.multiply.SQl.DBConnector.createConnection (line 150)");
            Logger.getLogger(DBAccess.class.getName()).log(Level.SEVERE, null, ex);
            connectionSuccessful = false;
        }

        return conn;
    }

    /**
     * Executes a query. Returns a ResultSet with the data obtained from the SQL
     * statement.
     *
     * @param conn The connection to which the query is being made.
     * @param sql The SELECT statement being queried.
     * @return The ResultSet generated from the SELECT.
     * @throws SQLException In the event that:<br>
     * The statement is of invalid syntax,<br>
     * The connection cannot be made,<br>
     * A ResultSet is generated.
     *
     */
    public ResultSet execQuery(Connection conn, String sql) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);

        return rs;
    }

    /**
     * Returns a Set (unique data) for a specific column, acquired from a
     * ResultSet, up to and including a number of results equal to the count
     * parameter.
     *
     * @param columnName Specific column in a table.
     * @param rs ResultSet of a table.
     * @param count Maximum number of results to be taken from the ResultSet.
     * @return Returns a Set containing unique data from a specific column.
     * @throws SQLException
     * @throws IOException
     */
    public Set returnResult(String columnName, ResultSet rs, int count) throws SQLException, IOException {
        int pos = 0;
        String time = ("" + new Timestamp(System.currentTimeMillis())).substring(0, 20);
        String temp = "";
        int[] rands = {4, 7, 10, 13, 16, 19};
        for (int i = 0; i < 20; i++) {
            boolean strangeChar = false;
            for (int j : rands) {
                if (i == j) {
                    strangeChar = true;
                }
            }
            if (!strangeChar) {
                temp += time.charAt(i);
            }
        }
        File results = new File("C:\\Users\\zachristophers\\Documents\\Results\\" + username + '\\' + columnName + '\\' + temp + ".csv");
        deleteFolder(results);
        results.getParentFile().mkdirs();
        PrintWriter fw = new PrintWriter(new FileWriter(results));
        ResultSetMetaData rsm = rs.getMetaData();
        for (int i = 1; i <= rsm.getColumnCount(); i++) {
            if (columnName.equalsIgnoreCase(rsm.getColumnName(i))) {
                pos = i;
            }
        }

        Vector vector = new Vector(1);

        while (rs.next() && count > 0) {
            vector.add(rs.getString(pos));
            count--;
            System.out.println(count + " " + columnName);
        }

        Set unique = new HashSet();
        unique.addAll(vector);

        for (Object i : unique) {
            fw.write('\'' + i.toString() + "\n");
        }
        fw.close();
        System.out.println("Success.");

        return unique;
    }

    /**
     * Deletes all existing parent folders for a clean set of data every time.
     *
     * @param folder The folder for which all data should be deleted.
     */
    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if (files != null) { //some JVMs return null for empty dirs
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }
}
