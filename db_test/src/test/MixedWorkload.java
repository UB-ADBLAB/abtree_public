package test;

import java.io.*;
import java.util.*;
import java.sql.*;

public class MixedWorkload {
	public static void main(String args[]) throws IOException {
		int args_base = 0;
		if (args.length >= 1 && args[0].equals("-i")) {
			SWRCountQuery.ask_for_samples_attempted = true;
			args_base = 1;
		}

		if (args.length - args_base < 12) {
			System.out.println(
				"usage: test.MixedWorkload [-i] <dbname> " +
				"<ninsert_threads> <seed> <insert_sleep_ms> " +
				"<insert_sleep_freq> <insert_initial_delay_ms> "+
				"<sample_size> <nquery_threads> " + 
				"<query_sleep_ms> <query_sleep_freq> " +
				"<query_initial_delay_ms> <total_time_s> [meterfile]");
				System.exit(1);
		}
        String url = "jdbc:postgresql://localhost:5432/" + args[args_base];
		int ninsert_threads = Integer.parseInt(args[args_base + 1]);
		long seed = Long.parseLong(args[args_base + 2]);
		long insert_sleep_ms = Long.parseLong(args[args_base + 3]);
		long insert_sleep_freq = Long.parseLong(args[args_base + 4]);
		long insert_initial_delay_ms = Long.parseLong(args[args_base + 5]);
		long sample_size = Long.parseLong(args[args_base + 6]);
		int nquery_threads = Integer.parseInt(args[args_base + 7]);
		long query_sleep_ms = Long.parseLong(args[args_base + 8]);
		long query_sleep_freq = Long.parseLong(args[args_base + 9]);
		long query_initial_delay_ms = Long.parseLong(args[args_base + 10]);
		long total_time_s = Long.parseLong(args[args_base + 11]);
		long total_time_ms = total_time_s * 1000;
		String meter_file;
		if (args.length - args_base > 12) {
			meter_file = args[args_base + 12];
		} else {
			Calendar now = Calendar.getInstance();
			meter_file = String.format(
				"MixedWorkload_%04d%02d%02d-%02d%02d%02d.meter",
				now.get(Calendar.YEAR),
				now.get(Calendar.MONTH) + 1,
				now.get(Calendar.DAY_OF_MONTH),
				now.get(Calendar.HOUR),
				now.get(Calendar.MINUTE),
				now.get(Calendar.SECOND));
		}

		Meter meter = new Meter(meter_file, 250);
		int samples_counter = meter.add_counter("#samples");
		int inserts_counter = meter.add_counter("#inserts");
		int nattempt_counter = -1;
		if (SWRCountQuery.ask_for_samples_attempted) {
			nattempt_counter = meter.add_counter("#attempts");
		}

		Random rng = new Random(seed);

		int total_num_threads = ninsert_threads + nquery_threads;
		Thread[] threads = new Thread[total_num_threads];
		
		// initialilze the threads
		for (int i = 0; i < total_num_threads; ++i) {
			// query threads go first
			if (i < nquery_threads) {
				threads[i] = new SWRCountQuery.QueryThread(
					i, url, false, -1, sample_size, meter,
					samples_counter, nattempt_counter,
					query_sleep_ms, query_sleep_freq);
			} else {
				threads[i] = new InsertManyRandom.QueryThread(
					i, url, false, -1, rng.nextLong(), meter,
					inserts_counter, insert_sleep_ms, insert_sleep_freq);
			}
		}
		
		// start the meter and threads
		meter.start();
		if (insert_initial_delay_ms > query_initial_delay_ms)
		{
			// Start the query threads first
			for (int i = 0; i < nquery_threads; ++i) {
				threads[i].start();
			}

			// wait for a while
			try {
				Thread.currentThread().sleep(insert_initial_delay_ms -
											 query_initial_delay_ms);
			} catch (InterruptedException e) {
				//??
			}
			total_time_ms -= (insert_initial_delay_ms - query_initial_delay_ms);

			// then start the insert threads
			for (int i = nquery_threads; i < total_num_threads; ++i) {
				threads[i].start();
			}
		}
		else
		{
			// Start the insert threads first
			for (int i = nquery_threads; i < total_num_threads; ++i) {
				threads[i].start();
			}

			// wait for a while
			if (insert_initial_delay_ms < query_initial_delay_ms)
			{
				try {
					Thread.currentThread().sleep(query_initial_delay_ms -
												 insert_initial_delay_ms);
				} catch (InterruptedException e) {
					//??
				}
			}
			total_time_ms -= (query_initial_delay_ms - insert_initial_delay_ms);

			// then start the insertion threads
			for (int i = 0; i < nquery_threads; ++i) {
				threads[i].start();
			}
		}

		// Sleep and finsh everthing...
		if (total_time_ms > 0) {
			try {
				Thread.currentThread().sleep(total_time_ms);
			} catch (InterruptedException e) {
				//??
			}
		}

		// Wait for finish...
		meter.signal_stop();
		for (int i = 0; i < total_num_threads; ++i) {
			try {
				// wake it up
				threads[i].interrupt();
				threads[i].join();
			} catch (InterruptedException e) {
				// why?
				--i;
				continue;
			}
		}
		meter.end();
		
		// Collect and print the final throughputs:
		long max_duration = 0;
		for (int i = 0; i < total_num_threads; ++i) {
			long duration;
			if (i < nquery_threads) {
				duration = ((SWRCountQuery.QueryThread) threads[i])
					.getInstance().getDurationMillis();
			} else {
				duration = ((InsertManyRandom.QueryThread) threads[i])
					.getInstance().getDurationMillis();
			}
			max_duration = Math.max(max_duration, duration);
		}

		long total_inserts = meter.get_counter(inserts_counter);
		long total_samples = meter.get_counter(samples_counter);
		double avg_insert_thruput = total_inserts / (max_duration / 1000.0);
		double avg_sample_thruput = total_samples / (max_duration / 1000.0);
		System.out.printf("Average throughput: %.3f inserts/second across %d " +
			"threads, %.3f samples/second across %d threads\n",
			avg_insert_thruput, ninsert_threads,
			avg_sample_thruput, nquery_threads);
		if (SWRCountQuery.ask_for_samples_attempted) {
			long N = meter.get_counter(nattempt_counter);
			System.out.printf("total samples attempted = %d\n", N);
			System.out.printf("acc rate = %.2f%%\n",
				100.0 * total_samples / N);
		}
	}
}
