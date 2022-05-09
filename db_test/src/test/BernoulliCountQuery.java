package test;

import java.util.*;
import java.sql.*;
import java.io.*;

public class BernoulliCountQuery extends QueryBase {

	public BernoulliCountQuery(String url, boolean use_si, Meter meter,
						 int counter_id) {
		m_meter = meter;
		m_counter_id = counter_id;

		Properties props = new Properties();
		setupConnection(url, use_si, props);
		/* turn on auto commit */
		try {
			conn().setAutoCommit(true);
		} catch (SQLException e) {
			System.out.println("unable to set autocommit to true");
			System.exit(1);
		}
	}

	public void run(int thread_id, long n, String sampling_rate) {
		System.out.printf("Query thread %d, n = %d, sampling_rate = %s%%\n", thread_id, n, sampling_rate);
		long i = 0;
		Connection conn = conn();

		long startTime = System.currentTimeMillis();
		try {
			PreparedStatement query_stmt = conn.prepareStatement(
				String.format("SELECT COUNT(*) FROM A TABLESAMPLE BERNOULLI (%s)", sampling_rate));
			
			for (i = 0; i < n; ++i) {
				ResultSet rs = query_stmt.executeQuery();
				long cnt;
				if (!rs.next())
				{
					System.out.printf("Something went wrong with query " +
						"thread %d run %d", thread_id, i);
				}
				cnt = rs.getLong(1);
				rs.close();

				if (m_meter != null) {
					m_meter.inc_counter(m_counter_id, cnt);
				}
			}
			query_stmt.close();
		} catch (SQLException e) {
			System.out.printf("ERROR %d [%s]: %s\n", e.getErrorCode(),
											   e.getSQLState(),
											   e.getMessage());
			System.out.printf("Last query i = %d\n", i);
		}
		long endTime = System.currentTimeMillis();

		System.out.printf("Query thread %d: time = %.3f s\n", thread_id,
						  (endTime - startTime) / 1e3);
		m_duration = endTime - startTime;
	}

	public long getDurationMillis() {
		return m_duration;
	}

	public static void main(String[] args) throws IOException {
		int args_base = 0;

		if (args.length - args_base < 4) {
			System.out.println("usage: test.BernoulliCountQuery <dbname> <n> " +
							   "<sampling_rate%> <num_threads> [meterfile]");
			System.exit(1);
		}

		String url = "jdbc:postgresql://localhost:5432/" + args[args_base];
		long n = Long.parseLong(args[args_base + 1]);
		String sampling_rate = args[args_base + 2];
		int num_threads = Integer.parseInt(args[args_base + 3]);
		String meter_file;
		if (args.length - args_base > 4) {
			meter_file = args[args_base + 4];
		} else {
			Calendar now = Calendar.getInstance();
			meter_file = String.format(
				"BernoulliCountQuery_%04d%02d%02d-%02d%02d%02d.meter",
				now.get(Calendar.YEAR),
				now.get(Calendar.MONTH) + 1,
				now.get(Calendar.DAY_OF_MONTH),
				now.get(Calendar.HOUR),
				now.get(Calendar.MINUTE),
				now.get(Calendar.SECOND));
		}

		Meter meter = new Meter(meter_file, 250);
		meter.add_counter("#samples");
		
		if (num_threads == 1) {
			System.out.println("Running single-threaded swr count query...");

			BernoulliCountQuery instance = new BernoulliCountQuery(url, false, meter, 0);
			if (!instance.connected()) {
				System.out.println("connection error\n");
				System.exit(1);
			}

			System.out.println("waiting... enter anything to start...");
			System.in.read();
			
			meter.start();
			instance.run(-1, n, sampling_rate);
			meter.end();
			instance.closeConn();

			long total_sampled = meter.get_counter(0);
			System.out.printf("total sampled = %d\n", total_sampled);
			long duration = instance.getDurationMillis();
			double thruput = (total_sampled) / (duration / 1000.0);
			System.out.printf("single-threaded random sampling throughput: " + 
				"%.3f samples/second\n", thruput);
			return ;
		}

		QueryThread[] query_threads = new QueryThread[num_threads];
		for (int i = 0; i < num_threads; ++i) {
			query_threads[i] =
				new QueryThread(i, url, false, n, sampling_rate, meter, 0);
		}
		
		meter.start();
		for (int i = 0; i < num_threads; ++i) {
			query_threads[i].start();
		}

		for (int i = 0; i < num_threads; ++i) {
			try {
				query_threads[i].join();
			} catch (InterruptedException e) {
				--i;
				continue;
			}
		}
		meter.end();
		
		long total_sampled = meter.get_counter(0);
		System.out.printf("total sampled = %d\n", total_sampled);
		System.out.println("Throughput:");
		long max_duration = 0;
		for (int i = 0; i < num_threads; ++i) {
			long duration = query_threads[i].getInstance().getDurationMillis();
			double thruput = (1.0 * total_sampled / num_threads) / (duration / 1000.0);
			max_duration = Math.max(max_duration, duration);
			System.out.printf("Query thread %d estimated: %.3f samples/second\n",
							  i, thruput);
		}
		double avg_thruput = (total_sampled * num_threads) / (max_duration / 1000.0);
		System.out.printf("average throughput = %.3f samples/second\n",
						  avg_thruput);
	}
	
	public static class QueryThread extends Thread {
		public QueryThread(int thread_id, String url, boolean use_si,
						   long n, String sampling_rate, Meter meter,
						   int counter_id) {
			m_thread_id = thread_id;
			m_url = url;
			m_use_si = use_si;
			m_n = n;
			m_sampling_rate = sampling_rate;
			m_meter = meter;
			m_counter_id = counter_id;
		}

		public void run() {
			m_instance = new BernoulliCountQuery(m_url, m_use_si, m_meter,
										   m_counter_id);
			if (!m_instance.connected()) {
				System.out.printf("Thread %d: connection error\n", m_thread_id);
				return ;
			}
			m_instance.run(m_thread_id, m_n, m_sampling_rate);
			m_instance.closeConn();
		}

		public BernoulliCountQuery getInstance() {
			return m_instance;
		}

		private int m_thread_id;
		private String m_url;
		private boolean m_use_si;
		private long m_n;
		private String m_sampling_rate;
		private Meter m_meter;
		private int m_counter_id;

		private BernoulliCountQuery m_instance;
	}

	private long m_duration; // millis
	private Meter m_meter;
	private int m_counter_id;
}

