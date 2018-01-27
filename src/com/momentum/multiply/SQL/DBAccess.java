/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.momentum.multiply.SQL;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;

/**
 * Intended to be accessing data in, and modifying the data found, in databases
 * connected by the DBConnector class.<br>
 * Future methods: UPDATE|ALTER TABLE|DELETE.<br>
 * This program is coded on the current JDK (8u51).<br>
 * <br>
 * Contact zachary.christophers@gmail.com for further updates. <br>
 * Please acknowledge the source of your code when using it.<br>
 *
 * @author Zachary Christophers
 * @version 1.0.1
 */
public class DBAccess {

    private DBConnector dbconn = null;
    /**
     * Private Set variables. Used in file comparison (see method filter()).
     *
     * @since version 1.0.0
     */
    private Set as400File = null, coreFile = null;

    /**
     * Data types found in a conventional database. Currently supported:
     * SMALLINT|INT|BIGINT|VARCHAR|BIT|DECIMAL
     *
     */
    public enum DATA_TYPES {
        VARCHAR,
        BIT,
        INT,
        SMALLINT,
        DECIMAL,
        BIGINT;
    }

    public enum DATABASES {
        AS400,
        DB2,
        SQLSERVER

    }

    /**
     * Constructor
     *
     * @throws java.lang.ClassNotFoundException
     * @throws java.sql.SQLException
     * @throws java.io.IOException
     */
    public DBAccess() throws ClassNotFoundException, SQLException, IOException {
        connectToDatabase();
    }

    /**
     * Constructor
     *
     * @throws java.lang.ClassNotFoundException
     * @throws java.sql.SQLException
     * @throws java.io.IOException
     */
    public DBAccess(String driver, String url, String username, String password) {
        connectToDatabase(driver, url, username, password);
    }

    /**
     * Creates a DBConnector which creates a connection to the database matching
     * dbname
     *
     * @param dbname Database name.
     * @return Returns a new database connection for the value at dbname.
     */
    public void connectToDatabase() {
        dbconn = new DBConnector();
    }

    public void connectToDatabase(String driver, String url, String username, String password) {
        try {
            if (url == null) {
                throw new IOException();
            }

            dbconn = new DBConnector(driver, url, username, password);
        } catch (FileNotFoundException fnfe) {
            System.out.println("connect to sql server failed. File not found.");
        } catch (IOException ex) {
            System.out.println("URL is null. Please re-enter the url.");
            Logger.getLogger(DBAccess.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Filters data from two files, generated from Sets for unique data. Writes
     * the comparison to a new file. Change This method to suit your own needs.
     *
     * @throws java.io.IOException IO Exception
     */
    public void filter() throws IOException {
        if (as400File != null && coreFile != null) {
            Set temp = as400File;
            temp.removeAll(coreFile);
            File results = new File("C:\\Users\\zachristophers\\Documents\\Results\\Empty Numbers\\Results\\Sparta.csv");
            DBConnector.deleteFolder(results);
            results.getParentFile().mkdirs();
            PrintWriter fw = new PrintWriter(new FileWriter(results));
            for (Object i : temp) {
                fw.write('\'' + i.toString() + "\n");
            }
            fw.close();
        }
    }//as400 and core specific

    /**
     * Populates the Sets at the top of the program
     *
     * @throws java.lang.ClassNotFoundException Class not found
     * @throws java.sql.SQLException SQL error
     * @throws java.io.IOException IO Exception
     */
    public void createResultSets() throws ClassNotFoundException, SQLException, IOException {
        String as400_SQL = "SELECT DISTINCT CRPOLA, CRPOLN, CRCNBR, CRRTYP, CCIDNR, CCDTOB, CCSEXC, TRIM(CCTITL) || ' ' || TRIM(CCFNAM) || ' ' || TRIM(CCSNAM), CONMLPCSTA\n"
                + "FROM BBLIB.CMSROLEPF A                                                                 \n"
                + "LEFT JOIN LPCPCONMLA D ON D.CONMLPNOAL = A.CRPOLA AND D.CONMLPNUMB = A.CRPOLN                        \n"
                + "LEFT JOIN BBLIB.CMSCLNTPF B ON B.CCCNBR = A.CRCNBR\n"
                + "WHERE crpola in ('MM','MB')\n"
                + "AND CONMLPCSTA = '10INFPPAY'\n"
                + "AND CRRTYP IN ('POLHOLD','PARTNER')\n"
                + "ORDER BY CRPOLA DESC";
        String core_SQL = "select * from raw_sch.RAW_RECORD_RECEIVED order by RECORD_RECEIVED_ID desc";
        DBConnector as400 = new DBConnector("as400");
        DBConnector core = new DBConnector("db2");

        as400File = as400.returnResult("CRCNBR", as400.execQuery(as400.createConnection(), as400_SQL), 100000);
        coreFile = core.returnResult("customer_number", core.execQuery(core.createConnection(), core_SQL), 200);

    }//as400 and core specific

    /**
     * Insert method. Inserts data into the given columns in the table
     * "tablename", found in the database "dbname". Data types match the
     * equivalent enum to the appropriate column as its data type. Manual or CSV
     * input.
     *
     * @param dbname Database name.
     * @param tablename Table name.
     * @param columns Columns into which data is being inserted.
     * @param datatypes Data types of the columns into which data is being
     * inserted.
     * @throws java.lang.ClassNotFoundException
     * @throws java.sql.SQLException
     */
    public void insertInto(String tablename, String[] columns, DATA_TYPES[] dt, String[][] data, int totalRecords) throws ClassNotFoundException, SQLException {
        StringBuilder insertStatement = new StringBuilder("INSERT INTO " + tablename + "(");

        for (int i = 0; i < columns.length; i++) {
            insertStatement.append(columns[i]);
            if (i + 1 != columns.length) {
                insertStatement.append(", ");
            }
        }
        insertStatement.append(")\n");

        for (int i = 0; i < totalRecords; i++) {
            insertStatement.append("VALUES(");
            for (int j = 0; j < columns.length; j++) {
                if (dt[j] == DATA_TYPES.VARCHAR) {
                    insertStatement.append(data[i][j]);
                } else if (dt[i] == DATA_TYPES.DECIMAL) {
                    insertStatement.append(Double.parseDouble(data[i][j]));
                } else if (dt[i] == DATA_TYPES.INT || dt[i] == DATA_TYPES.BIT) {
                    insertStatement.append(Integer.parseInt(data[i][j]));
                }
                if (j + 1 != columns.length) {
                    insertStatement.append(", ");
                }
            }
            insertStatement.append(")\n");
        }

        executeQuery(insertStatement.toString());

        System.out.println("Success.");
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        return dbconn.execQuery(dbconn.getConnection(), sql);
    }

    public boolean connectionSuccessful() {
        return dbconn.isConnectionSuccessful();
    }
}
