package test;

import java.util.*;
import java.sql.*;
import java.io.*;

public class RunQuery extends QueryBase {
    public static void main(String[] args)
		throws IOException, FileNotFoundException, SQLException {
        if (args.length < 2) {
            System.out.println("usage: RunQuery <dbname> <SQLFile>");
            System.exit(1);
        }
        String url = "jdbc:postgresql://localhost:5432/" + args[0];
        String sqlFile = args[1];

        Connection jdbcConn = getJDBCConn(url);
            
        InputStream in;
        if (sqlFile.equals("-")) {
            in = System.in; 
        } else {
            in = new FileInputStream(sqlFile); 
        }
        
        String query = readAllFromInputStream(in);

        Statement statement = jdbcConn.createStatement();
        statement.setFetchSize(10000);
        long startTime = System.currentTimeMillis();
        ResultSet result = statement.executeQuery(query);
        
        long count = 0;
        while (result.next()) {
            count += 1;
        }
        long endTime = System.currentTimeMillis();

        jdbcConn.commit();

        result.close(); 
        statement.close(); 
        jdbcConn.close();

        System.out.printf("count = %d, time = %.3f s\n", count, (endTime - startTime) / 1e3);
    }
}
