package test;

import java.util.*;
import java.sql.*;
import java.io.*;

public class DeleteManyRandom extends QueryBase {
	public DeleteManyRandom(String url, boolean use_si, Meter meter,
							int counter_id) {
		m_meter = meter;
		m_counter_id = counter_id;
		setupConnection(url, use_si);
	}

	public static final long commit_freq = 10000;

	public void run(int thread_id, long n, long seed, long sleep_ms,
													  long sleep_freq) {
		if (n == -1 && m_meter == null)  {
			System.out.println("[ERROR] delete thread: n = -1 and no meter provided");
			return ;
		}
		
		if (sleep_ms > 0 && sleep_freq <= 0) {
			sleep_freq = commit_freq;
			System.out.println("[INFO] delete thread: sleep_freq not given, defaults to commit_freq");
		}

		Connection	conn = this.conn();
		Random rng = new Random(seed);
		long i = 0;
		int x = -1, y = -1;

		System.out.printf("Delete thread %d, n = %d, seed = %d\n", thread_id, n, seed);

        long startTime = System.currentTimeMillis();
		try {
			PreparedStatement delete_stmt = conn.prepareStatement(
				"delete from A where y = ?");
			
			for (i = 1; n == -1 || i <= n; ++i) {
				x = rng.nextInt();
				y = rng.nextInt();
				delete_stmt.setInt(1, y);
				int num_deleted = delete_stmt.executeUpdate();

				// update the counter for every 100 deletes
				if (m_meter != null) {
					m_meter.inc_counter(m_counter_id, num_deleted);
				}

				if (i % commit_freq == 0) {
					conn.commit();
				}

				if (sleep_ms > 0 && i % sleep_freq == 0) {
					try {
						Thread.currentThread().sleep(sleep_ms);
					} catch (InterruptedException e) {
						if (n == -1 && m_meter.need_to_stop())
							break;

					}
				}
				
				// check for the stop condition every 1000 deletes
				if (n == -1 && i % 1000 == 0 && m_meter.need_to_stop())
					break;
			}
			
			if ((i - 1) % commit_freq != 0)
				conn.commit();
			delete_stmt.close();
		} catch (SQLException e) {
			System.out.printf("ERROR %d [%s]: %s\n", e.getErrorCode(),
												e.getSQLState(),
												e.getMessage());
			System.out.printf("Last delete: tuple %d = (%d, %d)\n", i, x, y);
		}
        long endTime = System.currentTimeMillis();
		
		// The last batch of updates.
		if (n % 100 != 0 && m_meter != null) {
			m_meter.inc_counter(m_counter_id, n - (n / 100 * 100));
		}

        System.out.printf("Delete thread %d: time = %.3f s\n", thread_id,
						  (endTime - startTime) / 1e3);
		m_duration = endTime - startTime;
	}

	public long getDurationMillis() {
		return m_duration;
	}

    public static void main(String[] args) throws IOException {
        if (args.length < 4) {
            System.out.println("usage: test.DeleteManyRandom <dbname> <n> " +
							   "<num_threads> <seed> [meterfile]");
            System.exit(1);
        }
        String url = "jdbc:postgresql://localhost:5432/" + args[0];
		long n = Long.parseLong(args[1]);
		int num_threads = Integer.parseInt(args[2]);
		long seed = Long.parseLong(args[3]);
		String meter_file;
		if (args.length > 4) {
			meter_file = args[4];
		} else {
			Calendar now = Calendar.getInstance();
			meter_file = String.format(
				"DeleteManyRandom_%04d%02d%02d-%02d%02d%02d.meter",
				now.get(Calendar.YEAR),
				now.get(Calendar.MONTH) + 1,
				now.get(Calendar.DAY_OF_MONTH),
				now.get(Calendar.HOUR),
				now.get(Calendar.MINUTE),
				now.get(Calendar.SECOND));
		}

		Meter meter = new Meter(meter_file, 250);
		meter.add_counter("#deletes");

		if (num_threads == 1) {
			System.out.println("Running single-threaded deleteer...");

			DeleteManyRandom instance = new DeleteManyRandom(url, false, meter,
															 0);
			if (!instance.connected()) {
				System.out.println("connection error\n");
				System.exit(1);
			}
			
			/* Waiting for attaching gdb to backend. */
			System.out.println("waiting... enter anything to start...");
			System.in.read();
			
			meter.start();
			instance.run(-1, n, seed, 0, 0);
			meter.end();
			instance.closeConn();

			long duration = instance.getDurationMillis();
			double thruput = n / (duration / 1000.0);
			System.out.printf("single-threaded delete throughput: %.3f " +
							  "deletes/second\n", thruput);
			return ;
		}
			
		Random rng = new Random(seed);
		QueryThread[] query_threads = new QueryThread[num_threads];
		for (int i = 0; i < num_threads; ++i) {
			query_threads[i] =
				new QueryThread(i, url, false, n, rng.nextLong(), meter, 0);
		}
		
		meter.start();
		for (int i = 0; i < num_threads; ++i) {
			query_threads[i].start();
		}

		for (int i = 0; i < num_threads; ++i) {
			try {
				query_threads[i].join();
			} catch(InterruptedException e) {
				--i;
				continue;
			}
		}
		meter.end();

		System.out.println("Throughput:");
		long max_duration = 0;
		for (int i = 0; i < num_threads; ++i) {
			long duration = query_threads[i].getInstance().getDurationMillis();
			double thruput = n / (duration / 1000.0);
			max_duration = Math.max(max_duration, duration);
			System.out.printf("Delete thread %d: %.3f deletes/second\n",
							  i, thruput);
		}
		double avg_thruput = n * num_threads / (max_duration / 1000.0);
		System.out.printf("average throughput = %.3f deletes/second\n",
						  avg_thruput);
    }

	public static class QueryThread extends Thread {
		public QueryThread(int thread_id, String url, boolean use_si,
						   long n, long seed, Meter meter, int counter_id) {
			m_thread_id = thread_id;
			m_url = url;
			m_use_si = use_si;
			m_n = n;
			m_seed = seed;
			m_meter = meter;
			m_counter_id = counter_id;
		}

		public QueryThread(int thread_id, String url, boolean use_si,
						   long n, long seed, Meter meter, int counter_id,
						   long sleep_ms, long sleep_freq) {
			m_thread_id = thread_id;
			m_url = url;
			m_use_si = use_si;
			m_n = n;
			m_seed = seed;
			m_meter = meter;
			m_counter_id = counter_id;
			m_sleep_ms = sleep_ms;
			m_sleep_freq = sleep_freq;
		}

		public void run() {
			m_instance = new DeleteManyRandom(m_url, m_use_si, m_meter,
											  m_counter_id);
			if (!m_instance.connected()) {
				System.out.printf("Thread %d: connection error\n", m_thread_id);
				return ;
			}

			m_instance.run(m_thread_id, m_n, m_seed, m_sleep_ms, m_sleep_freq);
			m_instance.closeConn();
		}

		public DeleteManyRandom getInstance() {
			return m_instance;
		}

		private int m_thread_id;
		private String m_url;
		private boolean m_use_si;
		private long m_n;
		private long m_seed;
		private Meter m_meter;
		private int m_counter_id;
		private long m_sleep_ms;
		private long m_sleep_freq;

		private DeleteManyRandom m_instance;
	}

	private long m_duration; /* in milliseconds */
	private Meter m_meter;
	private int m_counter_id;
}
