package test;

import java.util.*;
import java.sql.*;
import java.io.*;

public class InsertMany extends QueryBase {
	public InsertMany(String url, boolean use_si) {
		setupConnection(url, use_si);
	}

	public void run(long n) {
        Connection jdbcConn = conn();
		long startTime = 0;
		long endTime = 0;
		long i = -1;
		int x = -1, y = -1;
		
		try {
			PreparedStatement insert_stmt = jdbcConn.prepareStatement(
				"insert into A values (?, ?)");
			startTime = System.currentTimeMillis();
			for (i = 0; i < n; ++i) {
				x = 10000000 + (int) i;
				y = 60000000 + (int) i;
				insert_stmt.setInt(1, x);
				insert_stmt.setInt(2, y);
				if (insert_stmt.executeUpdate() != 1) {
					System.out.printf("failed to insert (%ld, %ld)\n", x, y);
				}
			}
			jdbcConn.commit();
			endTime = System.currentTimeMillis();
			insert_stmt.close();
		} catch (SQLException e) {
			System.out.printf("ERROR %d: %s\n", e.getErrorCode(),
												e.getSQLState());
			System.out.printf("Last insert: tuple %d = (%d, %d)\n", i, x, y);
		}

        System.out.printf("time = %.3f s\n", (endTime - startTime) / 1e3);
	}

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("usage: RunQuery <dbname> <n>");
            System.exit(1);
        }
        String url = "jdbc:postgresql://localhost:5432/" + args[0];
		long n = Long.parseLong(args[1]);

		InsertMany instance = new InsertMany(url, false);
		if (!instance.connected()) {
			System.out.println("Connection error\n");
			System.exit(1);
		}
		
		System.out.println("waiting for continue? enter anything");
		System.in.read();

		instance.run(n);
		instance.closeConn();
    }
}
