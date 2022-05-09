package test;

import java.io.*;
import java.util.*;
import java.sql.*;

public class MixedDeleteAndSWRCount {
	public static void main(String args[]) throws IOException {
		int args_base = 0;
		if (args.length >= 1 && args[0].equals("-i")) {
			SWRCountQuery.ask_for_samples_attempted = true;
			args_base = 1;
		}

		if (args.length - args_base < 7) {
			System.out.println(
				"usage: test.MixedDeleteAndSWRCount [-i] <dbname> " +
				"<ndeletes> <ndelete_threads`> <seed> " +
				"<nqueries> <sample_size> <nquery_threads> [meterfile]");
				System.exit(1);
		}
        String url = "jdbc:postgresql://localhost:5432/" + args[args_base];
		long ndeletes = Long.parseLong(args[args_base + 1]);
		int ndelete_threads = Integer.parseInt(args[args_base + 2]);
		long seed = Long.parseLong(args[args_base + 3]);
		long nqueries = Long.parseLong(args[args_base + 4]);
		long sample_size = Long.parseLong(args[args_base + 5]);
		int nquery_threads = Integer.parseInt(args[args_base + 6]);
		String meter_file;
		if (args.length - args_base > 7) {
			meter_file = args[args_base + 7];
		} else {
			Calendar now = Calendar.getInstance();
			meter_file = String.format(
				"MixedDeleteAndSWRCount_%04d%02d%02d-%02d%02d%02d.meter",
				now.get(Calendar.YEAR),
				now.get(Calendar.MONTH) + 1,
				now.get(Calendar.DAY_OF_MONTH),
				now.get(Calendar.HOUR),
				now.get(Calendar.MINUTE),
				now.get(Calendar.SECOND));
		}

		Meter meter = new Meter(meter_file, 250);
		int samples_counter = meter.add_counter("#samples");
		int deletes_counter = meter.add_counter("#deletes");
		int nattempt_counter = -1;
		if (SWRCountQuery.ask_for_samples_attempted) {
			nattempt_counter = meter.add_counter("#attempts");
		}

		System.out.printf("%d %d %d\n", samples_counter, deletes_counter, nattempt_counter);

		Random rng = new Random(seed);

		int total_num_threads = ndelete_threads + nquery_threads;
		Thread[] threads = new Thread[total_num_threads];
		
		// initialilze the threads
		for (int i = 0; i < total_num_threads; ++i) {
			// query threads go first
			if (i < nquery_threads) {
				threads[i] = new SWRCountQuery.QueryThread(
					i, url, false, nqueries, sample_size, meter,
					samples_counter, nattempt_counter);
			} else {
				threads[i] = new DeleteManyRandom.QueryThread(
					i, url, false, ndeletes, rng.nextLong(), meter,
					deletes_counter);
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
		for (int i = 0; i < total_num_threads; ++i) {
			long duration;
			if (i < nquery_threads) {
				duration = ((SWRCountQuery.QueryThread) threads[i])
					.getInstance().getDurationMillis();
				double thruput = (nqueries * sample_size) / (duration / 1000.0);
				System.out.printf("Query thread %d: %.3f samples/second\n",
					i, thruput);
			} else {
				duration = ((DeleteManyRandom.QueryThread) threads[i])
					.getInstance().getDurationMillis();
				double thruput = (ndeletes) / (duration / 1000.0);
				System.out.printf("Delete thread %d: %.3f deletes/second\n",
					i, thruput);
			}
			max_duration = Math.max(max_duration, duration);
		}
		double avg_delete_thruput =
			(ndelete_threads * ndeletes) / (max_duration / 1000.0);
		double avg_sample_thruput =
			(nquery_threads * nqueries * sample_size) / (max_duration / 1000.0);
		System.out.printf("Average throughput: %.3f deletes/second across %d " +
			"threads, %.3f samples/second across %d threads\n",
			avg_delete_thruput, ndelete_threads,
			avg_sample_thruput, nquery_threads);
		if (SWRCountQuery.ask_for_samples_attempted) {
			long N = meter.get_counter(nattempt_counter);
			System.out.printf("total samples attempted = %d\n", N);
			System.out.printf("acc rate = %.2f%%\n",
				100.0 * nquery_threads * nqueries * sample_size / N);
		}
	}
}
