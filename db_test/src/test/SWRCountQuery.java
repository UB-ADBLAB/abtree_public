package test;

import java.util.*;
import java.sql.*;
import java.io.*;

public class SWRCountQuery extends QueryBase {

	public static boolean ask_for_samples_attempted = false;
	
	public SWRCountQuery(String url, boolean use_si, Meter meter,
						 int counter_id, int nattempt_counter_id) {
		m_meter = meter;
		m_counter_id = counter_id;
		m_nattempt_counter_id = nattempt_counter_id;

		Properties props = new Properties();
		
		// don't have to turn off seq_scan and bitmapscan because
		// tablesample swr() range table is forced to choose index path now..
		//props.setProperty("options", "-c enable_seq_scan=off " +
									 //"-c enable_bitmapscan=off");
		StringBuilder options_builder = new StringBuilder();
		options_builder.append("-c tablesample_swr_ask_for_samples_attempted=");
		if (ask_for_samples_attempted) {
			options_builder.append("on");
		} else {
			options_builder.append("off");
		}
		props.setProperty("options", options_builder.toString());
		setupConnection(url, use_si, props);
		/* turn on auto commit */
		try {
			conn().setAutoCommit(true);
		} catch (SQLException e) {
			System.out.println("unable to set autocommit to true");
			System.exit(1);
		}
	}

	public void run(int thread_id, long n, long sample_size,
					long sleep_ms, long sleep_freq) {
		if (n == -1 && m_meter == null) {
			System.out.println("[ERROR] query thread: n = -1 and no meter provided");
			return ;
		}
		if (sleep_ms > 0 && sleep_freq <= 0) {
			System.out.println("[ERROR] positive sleep_freq expected with sleep_ms > 0");
			return ;
		}

		System.out.printf("Query thread %d, n = %d, sample_size = %d\n", thread_id, n, sample_size);
		long i = 0;
		Connection conn = conn();

		long startTime = System.currentTimeMillis();
		try {
			PreparedStatement query_stmt = conn.prepareStatement(
				String.format("SELECT COUNT(*) FROM A TABLESAMPLE SWR (%d)", sample_size));
			
			for (i = 1; n == - 1 || i <= n; ++i) {
				ResultSet rs = query_stmt.executeQuery();
				if (!rs.next() || rs.getInt(1) != sample_size || rs.next())
				{
					System.out.printf("Something went wrong with query " +
						"thread %d run %d", thread_id, i);
				}
				if (ask_for_samples_attempted && m_meter != null) {
					SQLWarning warning = query_stmt.getWarnings();
					while (warning != null) {
						String str = warning.getMessage();
						if (str.startsWith("N=")) {
							long N;
							try {	
								N = Long.parseLong(str.substring(2));
							} catch (Exception e) {
								N = n;
							}
							m_meter.inc_counter(m_nattempt_counter_id, N);
							break;
						}
						warning = warning.getNextWarning();
					}
				}
				rs.close();

				if (m_meter != null) {
					m_meter.inc_counter(m_counter_id, sample_size);
				}

				if (sleep_ms > 0 && i % sleep_freq == 0) {
					try {
						Thread.currentThread().sleep(sleep_ms);
					} catch (InterruptedException e) {
					}
				}

				if (n == -1)
				{
					if (m_meter.need_to_stop())
						break;
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
		if (args.length >= 1 && args[0].equals("-i")) {
			ask_for_samples_attempted = true;
			args_base = 1;
		}

		if (args.length - args_base < 4) {
			System.out.println("usage: test.SWRCountQuery [-i] <dbname> <n> " +
							   "<sample_size> <num_threads> [meterfile]");
			System.exit(1);
		}

		String url = "jdbc:postgresql://localhost:5432/" + args[args_base];
		long n = Long.parseLong(args[args_base + 1]);
		long sample_size = Long.parseLong(args[args_base + 2]);
		int num_threads = Integer.parseInt(args[args_base + 3]);
		String meter_file;
		if (args.length - args_base > 4) {
			meter_file = args[args_base + 4];
		} else {
			Calendar now = Calendar.getInstance();
			meter_file = String.format(
				"SWRCountQuery_%04d%02d%02d-%02d%02d%02d.meter",
				now.get(Calendar.YEAR),
				now.get(Calendar.MONTH) + 1,
				now.get(Calendar.DAY_OF_MONTH),
				now.get(Calendar.HOUR),
				now.get(Calendar.MINUTE),
				now.get(Calendar.SECOND));
		}

		Meter meter = new Meter(meter_file, 250);
		meter.add_counter("#samples");
		if (ask_for_samples_attempted) {
			meter.add_counter("#attempts");
		}
		
		if (num_threads == 1) {
			System.out.println("Running single-threaded swr count query...");

			SWRCountQuery instance = new SWRCountQuery(url, false, meter, 0, 1);
			if (!instance.connected()) {
				System.out.println("connection error\n");
				System.exit(1);
			}

			System.out.println("waiting... enter anything to start...");
			System.in.read();
			
			meter.start();
			instance.run(-1, n, sample_size, 0, 0);
			meter.end();
			instance.closeConn();

			long duration = instance.getDurationMillis();
			double thruput = (n * sample_size) / (duration / 1000.0);
			System.out.printf("single-threaded random sampling throughput: " + 
				"%.3f samples/second\n", thruput);
			return ;
		}

		QueryThread[] query_threads = new QueryThread[num_threads];
		for (int i = 0; i < num_threads; ++i) {
			query_threads[i] =
				new QueryThread(i, url, false, n, sample_size, meter, 0, 1);
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

		System.out.println("Throughput:");
		long max_duration = 0;
		for (int i = 0; i < num_threads; ++i) {
			long duration = query_threads[i].getInstance().getDurationMillis();
			double thruput = (n * sample_size) / (duration / 1000.0);
			max_duration = Math.max(max_duration, duration);
			System.out.printf("Query thread %d: %.3f samples/second\n",
							  i, thruput);
		}
		double avg_thruput = (n * sample_size * num_threads) / (max_duration / 1000.0);
		System.out.printf("average throughput = %.3f samples/second\n",
						  avg_thruput);
		if (ask_for_samples_attempted) {
			long N = meter.get_counter(1);
			System.out.printf("total samples attempted = %d\n", N);
			System.out.printf("acc rate = %.2f%%\n",
				100.0 * n * sample_size * num_threads / N);
		}
	}
	
	public static class QueryThread extends Thread {
		public QueryThread(int thread_id, String url, boolean use_si,
						   long n, long sample_size, Meter meter,
						   int counter_id, int nattempt_counter_id) {
			m_thread_id = thread_id;
			m_url = url;
			m_use_si = use_si;
			m_n = n;
			m_sample_size = sample_size;
			m_meter = meter;
			m_counter_id = counter_id;
			m_nattempt_counter_id = nattempt_counter_id;
		}

		public QueryThread(int thread_id, String url, boolean use_si,
						   long n, long sample_size, Meter meter,
						   int counter_id, int nattempt_counter_id,
						   long sleep_ms, long sleep_freq) {
			m_thread_id = thread_id;
			m_url = url;
			m_use_si = use_si;
			m_n = n;
			m_sample_size = sample_size;
			m_meter = meter;
			m_counter_id = counter_id;
			m_nattempt_counter_id = nattempt_counter_id;
			m_sleep_ms = sleep_ms;
			m_sleep_freq = sleep_freq;
		}

		public void run() {
			m_instance = new SWRCountQuery(m_url, m_use_si, m_meter,
										   m_counter_id, m_nattempt_counter_id);
			if (!m_instance.connected()) {
				System.out.printf("Thread %d: connection error\n", m_thread_id);
				return ;
			}
			m_instance.run(m_thread_id, m_n, m_sample_size, m_sleep_ms,
						   m_sleep_freq);
			m_instance.closeConn();
		}

		public SWRCountQuery getInstance() {
			return m_instance;
		}

		private int m_thread_id;
		private String m_url;
		private boolean m_use_si;
		private long m_n;
		private long m_sample_size;
		private Meter m_meter;
		private int m_counter_id;
		private int m_nattempt_counter_id;
		private long m_sleep_ms;
		private long m_sleep_freq;

		private SWRCountQuery m_instance;
	}

	private long m_duration; // millis
	private Meter m_meter;
	private int m_counter_id;
	private int m_nattempt_counter_id;
}

