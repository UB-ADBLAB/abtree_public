package test;

import java.util.*;
import java.sql.*;
import java.io.*;

public class QueryBase {
	public static Connection getJDBCConn(String url) {
		return getJDBCConn(url, new Properties());
	}

    public static Connection getJDBCConn(String url, Properties props) {
        try {
			if (props.getProperty("user") == null)
				props.setProperty("user", System.getenv("USER"));
			if (props.getProperty("password") == null)
				props.setProperty("password", "");

			/* Always use server-side PREPARE by default. */
			if (props.getProperty("prepareThreshold") == null)
				props.setProperty("prepareThreshold", "1");

            String driverName = "org.postgresql.Driver";

            Class.forName(driverName);
            Connection jdbcConnection = DriverManager.getConnection(url, props);
            return jdbcConnection;
        } catch (ClassNotFoundException | SQLException e){
            System.err.println("connection error");
        }
        return null;
    }

    public static String readAllFromInputStream(InputStream in) {
        Scanner scanner = new Scanner(in);
        scanner.useDelimiter("\\Z");
        return scanner.next();
    }

	public void setupConnection(String url, boolean use_si) {
		setupConnection(url, use_si, new Properties());
	}

	public void setupConnection(String url, boolean use_si, Properties props) {
		m_conn = getJDBCConn(url, props);

		try {
			/* turn off auto commit by default. */
			m_conn.setAutoCommit(false);
			if (use_si)
				m_conn.setTransactionIsolation(
					Connection.TRANSACTION_REPEATABLE_READ);
			else
				m_conn.setTransactionIsolation(
					Connection.TRANSACTION_READ_COMMITTED);
		} catch (SQLException e) {

		}
	}

	public Connection conn() { return m_conn; }

	public boolean connected() { return m_conn != null; }

	public void closeConn() {
		try {
			m_conn.close();
		} catch (SQLException e) {

		}
		m_conn = null;
	}

	private Connection m_conn;
}
