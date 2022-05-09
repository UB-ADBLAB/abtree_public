package test;

import java.io.*;
import java.util.*;
import java.sql.*;

public class MixedTPCHInsertAndBernoulli {
	public static void main(String args[]) throws IOException {
		int args_base = 0;

		if (args.length - args_base < 9) {
			System.out.println(
				"usage: test.MixedTPCHInsertAndBernoulli <dbname> " +
				"<ninserts> <ninsert_threads> <seed> <insert_init_sleep_ms> " +
				"<nqueries> <sample_percentage%> <nquery_threads> <query_init_sleep_ms> [meterfile]");
				System.exit(1);
		}
        String url = "jdbc:postgresql://localhost:5432/" + args[args_base];
		long ninserts = Long.parseLong(args[args_base + 1]);
		int ninsert_threads = Integer.parseInt(args[args_base + 2]);
		long seed = Long.parseLong(args[args_base + 3]);
		long insert_init_sleep_ms = Long.parseLong(args[args_base + 4]);
		long nqueries = Long.parseLong(args[args_base + 5]);
		String sampling_rate = args[args_base + 6];
		int nquery_threads = Integer.parseInt(args[args_base + 7]);
		long query_init_sleep_ms = Long.parseLong(args[args_base + 8]);
		String meter_file;
		if (args.length - args_base > 9) {
			meter_file = args[args_base + 9];
		} else {
			Calendar now = Calendar.getInstance();
			meter_file = String.format(
				"MixedTPCHInsertAndBernoulli_%04d%02d%02d-%02d%02d%02d.meter",
				now.get(Calendar.YEAR),
				now.get(Calendar.MONTH) + 1,
				now.get(Calendar.DAY_OF_MONTH),
				now.get(Calendar.HOUR),
				now.get(Calendar.MINUTE),
				now.get(Calendar.SECOND));
		}

		System.out.printf("insert_init_sleep_ms = %d, query_init_sleep_ms = %d\n",
						  insert_init_sleep_ms, query_init_sleep_ms);

		Meter meter = new Meter(meter_file, 250);
		int samples_counter = meter.add_counter("#samples");
		int inserts_counter = meter.add_counter("#inserts");
		int nqueries_counter = meter.add_counter("#queries");

		System.out.printf("%d %d %d\n", samples_counter, inserts_counter, nqueries_counter);

		Random rng = new Random(seed);

		int total_num_threads = ninsert_threads + nquery_threads;
		Thread[] threads = new Thread[total_num_threads];
		
		// initialilze the threads
		for (int i = 0; i < total_num_threads; ++i) {
			// query threads go first
			if (i < nquery_threads) {
				threads[i] = new BernoulliTPCHQuery.QueryThread(
					i, url, false, nqueries, sampling_rate, meter,
					samples_counter, nqueries_counter,
					query_init_sleep_ms, 0, rng.nextLong());
			} else {
				threads[i] = new InsertTPCHRandom.QueryThread(
					i, url, false, ninserts, rng.nextLong(), meter,
					inserts_counter, insert_init_sleep_ms, 0);
			}
		}
		
		// Start the threads
		meter.start();
		for (int i = 0; i < total_num_threads; ++i) {
			threads[i].start();
		}

		// Wait for finish...
		for (int i = 0; i < total_num_threads; ++i) {
			try {
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
		long total_sampled = meter.get_counter(samples_counter);
		for (int i = 0; i < total_num_threads; ++i) {
			long duration;
			if (i < nquery_threads) {
				duration = ((BernoulliTPCHQuery.QueryThread) threads[i])
					.getInstance().getDurationMillis();
				double thruput = (total_sampled * 1.0 / nquery_threads) / (duration / 1000.0);
				System.out.printf("Query thread %d estimated: %.3f samples/second\n",
					i, thruput);
			} else {
				duration = ((InsertTPCHRandom.QueryThread) threads[i])
					.getInstance().getDurationMillis();
				double thruput = (ninserts) / (duration / 1000.0);
				System.out.printf("Insert thread %d: %.3f inserts/second\n",
					i, thruput);
			}
			max_duration = Math.max(max_duration, duration);
		}
		double avg_insert_thruput =
			(ninsert_threads * ninserts) / (max_duration / 1000.0);
		double avg_sample_thruput =
			(total_sampled * 1.0) / (max_duration / 1000.0);
		System.out.printf("Average throughput: %.3f inserts/second across %d " +
			"threads, %.3f samples/second across %d threads\n",
			avg_insert_thruput, ninsert_threads,
			avg_sample_thruput, nquery_threads);
	}
}
