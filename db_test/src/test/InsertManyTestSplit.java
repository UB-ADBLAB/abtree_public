package test;

import java.util.*;
import java.sql.*;
import java.io.*;

public class InsertManyTestSplit extends QueryBase {
	public InsertManyTestSplit(String url, boolean use_si) {
		setupConnection(url, use_si);
	}

	public void run() {
        Connection jdbcConn = conn();
		long startTime = 0;
		long endTime = 0;
		long i = -1;
		int x = -1, y = -1;
		long n = 291;
		
		try {
			PreparedStatement insert_stmt = jdbcConn.prepareStatement(
				"insert into A values (?, ?)");
			startTime = System.currentTimeMillis();
			for (i = 0; i < n; ++i) {
				if (i < 290 && i != 259 && i != 260) {
					x = 10000000 + ((int) i) * 2;
					y = 60000000 + ((int) i) * 2;
				} else {
					x = 10000000 + (259 * 2);
					y = 60000000 + (259 * 2);
				}
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
        if (args.length < 1) {
            System.out.println("usage: test.InsertManyTestSplit <dbname>");
            System.exit(1);
        }
        String url = "jdbc:postgresql://localhost:5432/" + args[0];

		InsertManyTestSplit instance = new InsertManyTestSplit(url, false);
		if (!instance.connected()) {
			System.out.println("Connection error\n");
			System.exit(1);
		}
		
		System.out.println("waiting for continue? enter anything");
		System.in.read();

		instance.run();
		instance.closeConn();
    }
}
