package test;

import java.util.*;
import java.sql.*;
import java.io.*;
import java.time.LocalDate;
import java.time.temporal.JulianFields;

public class BernoulliTPCHQuery extends QueryBase {

	public BernoulliTPCHQuery(String url, boolean use_si, Meter meter,
						 int counter_id, int nqueries_counter_id) {
		m_meter = meter;
		m_counter_id = counter_id;
		m_nqueries_counter_id = nqueries_counter_id;

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

	// 1992-01-01, min shipdate
	public static final int min_j_shipdate = 2448623;

	// 1997-12-01, max shipdate lower bound
	public static final int max_j_shipdate = 2450784;


	public void run(int thread_id, long n, String sampling_rate,
					long sleep_ms, long sleep_freq, long seed) {
		System.out.printf("Query thread %d, n = %d, sampling_rate = %s%%\n", thread_id, n, sampling_rate);

		if (sleep_ms > 0 && sleep_freq < 0) {
			System.out.println("[ERROR] positive sleep_freq expected with sleep_ms > 0");
			return ;
		}

		if (sleep_ms > 0 && sleep_freq == 0) {
			// use sleep_freq == 0 to indicate we want the thread to sleep
			// before running anything
			try {
				Thread.currentThread().sleep(sleep_ms);
			} catch (InterruptedException e) {
			}
			sleep_ms = 0;
		}

		Connection conn = conn();

		long i = 0;
		long startTime = System.currentTimeMillis();
		long lastTime = startTime;
		Random rng = new Random(seed);
		long shipdate = -1;

		try {
			PreparedStatement query_stmt = conn.prepareStatement(
				String.format("SELECT COUNT(*), AVG(l_extendedprice) FROM lineitem TABLESAMPLE BERNOULLI (%s) WHERE l_shipdate >= ? and l_shipdate < ?",
							  sampling_rate));
			
			for (i = 0; i < n; ++i) {
				if (rng.nextInt(10) == 0) {
					// 10% chance to have query any 2-year interval
					shipdate = rng.nextInt(max_j_shipdate - min_j_shipdate + 1 - 365);
					shipdate += min_j_shipdate;
				} else {
					// 90% chance to query in any 2-year interval ending in
					// the last valid shipdate year (1997-12-02 to 1998-12-01)
					shipdate = rng.nextInt(365 + 1);	
					shipdate += max_j_shipdate - 365;
				}

				LocalDate shipdate_d = LocalDate.MIN.with(JulianFields.JULIAN_DAY, shipdate);
				java.sql.Date shipdate_sql_d = new java.sql.Date(
					shipdate_d.getYear() - 1900,
					shipdate_d.getMonthValue() - 1,
					shipdate_d.getDayOfMonth());
				query_stmt.setDate(1, shipdate_sql_d);

				LocalDate shipdate_d2 = shipdate_d.plusYears(1);
				java.sql.Date shipdate_sql_d2 = new java.sql.Date(
					shipdate_d2.getYear() - 1900,
					shipdate_d2.getMonthValue() - 1,
					shipdate_d2.getDayOfMonth());
				query_stmt.setDate(2, shipdate_sql_d2);

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
					m_meter.inc_counter(m_nqueries_counter_id, 1);
				}
				
				long curTime = System.currentTimeMillis();
				System.out.printf("Query thread %d, query %d: time = %.3f s\n",
						thread_id, i, (curTime - lastTime) / 1e3);
				lastTime = curTime;
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

		if (args.length - args_base < 5) {
			System.out.println("usage: test.BernoulliTPCHQuery <dbname> <n> " +
							   "<sampling_rate%> <num_threads> <seed> [meterfile]");
			System.exit(1);
		}

		String url = "jdbc:postgresql://localhost:5432/" + args[args_base];
		long n = Long.parseLong(args[args_base + 1]);
		String sampling_rate = args[args_base + 2];
		int num_threads = Integer.parseInt(args[args_base + 3]);
		long seed = Long.parseLong(args[args_base + 4]);
		String meter_file;
		if (args.length - args_base > 5) {
			meter_file = args[args_base + 5];
		} else {
			Calendar now = Calendar.getInstance();
			meter_file = String.format(
				"BernoulliTPCHQuery_%04d%02d%02d-%02d%02d%02d.meter",
				now.get(Calendar.YEAR),
				now.get(Calendar.MONTH) + 1,
				now.get(Calendar.DAY_OF_MONTH),
				now.get(Calendar.HOUR),
				now.get(Calendar.MINUTE),
				now.get(Calendar.SECOND));
		}

		Meter meter = new Meter(meter_file, 250);
		int nsamples_counter = meter.add_counter("#samples");
		int nqueries_counter = meter.add_counter("#queries");
		
		if (num_threads == 1) {
			System.out.println("Running single-threaded swr count query...");

			BernoulliTPCHQuery instance = new BernoulliTPCHQuery(
				url, false, meter, nsamples_counter, nqueries_counter);
			if (!instance.connected()) {
				System.out.println("connection error\n");
				System.exit(1);
			}

			System.out.println("waiting... enter anything to start...");
			System.in.read();
			
			meter.start();
			instance.run(-1, n, sampling_rate, 0, 0, seed);
			meter.end();
			instance.closeConn();

			long total_sampled = meter.get_counter(nsamples_counter);
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
				new QueryThread(i, url, false, n, sampling_rate, meter,
								nsamples_counter, nqueries_counter,
								i + seed);
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
		
		long total_sampled = meter.get_counter(nsamples_counter);
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
						   int counter_id, int nqueries_counter_id,
						   long seed) {
			m_thread_id = thread_id;
			m_url = url;
			m_use_si = use_si;
			m_n = n;
			m_sampling_rate = sampling_rate;
			m_meter = meter;
			m_counter_id = counter_id;
			m_nqueries_counter_id = nqueries_counter_id;
			m_sleep_ms = 0;
			m_sleep_freq = 0;
			m_seed = seed;
		}

		public QueryThread(int thread_id, String url, boolean use_si,
						   long n, String sampling_rate, Meter meter,
						   int counter_id, int nqueries_counter_id,
						   long sleep_ms, long sleep_freq,
						   long seed) {
			m_thread_id = thread_id;
			m_url = url;
			m_use_si = use_si;
			m_n = n;
			m_sampling_rate = sampling_rate;
			m_meter = meter;
			m_counter_id = counter_id;
			m_nqueries_counter_id = nqueries_counter_id;
			m_sleep_ms = sleep_ms;
			m_sleep_freq = sleep_freq;
			m_seed = seed;
		}

		public void run() {
			m_instance = new BernoulliTPCHQuery(m_url, m_use_si, m_meter,
										   m_counter_id, m_nqueries_counter_id);
			if (!m_instance.connected()) {
				System.out.printf("Thread %d: connection error\n", m_thread_id);
				return ;
			}
			m_instance.run(m_thread_id, m_n, m_sampling_rate,
						   m_sleep_ms, m_sleep_freq, m_seed);
			m_instance.closeConn();
		}

		public BernoulliTPCHQuery getInstance() {
			return m_instance;
		}

		private int m_thread_id;
		private String m_url;
		private boolean m_use_si;
		private long m_n;
		private String m_sampling_rate;
		private Meter m_meter;
		private int m_counter_id;
		private int m_nqueries_counter_id;
		private long m_seed;
		private long m_sleep_ms;
		private long m_sleep_freq;

		private BernoulliTPCHQuery m_instance;
	}

	private long m_duration; // millis
	private Meter m_meter;
	private int m_counter_id;
	private int m_nqueries_counter_id;
}

