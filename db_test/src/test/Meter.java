package test;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public class Meter {
	public Meter(String outfile, long report_interval) {
		m_outfile = outfile;
		m_report_interval = report_interval;
		m_desc = new ArrayList<String>();
		m_counters = new ArrayList<AtomicLong>();
		m_stopped = new AtomicBoolean();
		m_need_to_stop = new AtomicBoolean();
		m_thread = new Thread(() -> {
			this.run();
		});
	}

	public int add_counter(String desc) {
		m_desc.add(desc);
		m_counters.add(new AtomicLong());
		return m_desc.size() - 1;
	}

	public void inc_counter(int idx) {
		m_counters.get(idx).incrementAndGet();
	}

	public void inc_counter(int idx, long delta) {
		m_counters.get(idx).addAndGet(delta);
	}

	public void dec_counter(int idx) {
		m_counters.get(idx).decrementAndGet();
	}

	public void dec_counter(int idx, long delta) {
		m_counters.get(idx).addAndGet(-delta);
	}

	public long get_counter(int idx) {
		return m_counters.get(idx).get();
	}

	public void start() {
		m_need_to_stop.set(false);
		m_stopped.set(false);
		m_thread.start();
	}

	public void end() {
		m_stopped.set(true);
		try {
			// try to wake up the thread if it's sleeping
			m_thread.interrupt();
		} catch (Exception e) {
		}
		while (true) {
			try {
				m_thread.join();
				break;
			} catch (InterruptedException e) {
			}
		}
	}

	public void signal_stop() {
		m_need_to_stop.set(true);
	}

	public boolean need_to_stop() {
		return m_need_to_stop.get();
	}

	private void run() {
		PrintStream out = null;
		
		try {
			out = new PrintStream(new BufferedOutputStream(
					new FileOutputStream(m_outfile)));
		} catch (FileNotFoundException e) {
			System.out.printf("[METER THREAD] unable to create file %s\n",
							  m_outfile);
		}
		int n = m_desc.size();
		out.printf("Time (s)");
		for (int i = 0; i < n; ++i) {
			out.printf("\t%s", m_desc.get(i));
		}
		out.println();
		out.flush();
		
		long begin_tp = System.currentTimeMillis();
		long last_tp = begin_tp;
		while (!m_stopped.get()) {
			long target_tp = last_tp + m_report_interval;
			long mod = (target_tp - begin_tp) % m_report_interval;
			if (mod != 0) {
				target_tp -= mod;	
			}
			long sleep_time = target_tp - System.currentTimeMillis();	
			while (sleep_time > 0) {
				try {
					Thread.currentThread().sleep(sleep_time);
				} catch (InterruptedException e) {
				}
				last_tp = System.currentTimeMillis();
				sleep_time = target_tp - last_tp;
				// break out of the loop early if we are asked to stop
				if (m_stopped.get()) break;
			}

			/* ok report the counters. */
			out.printf("%.3f", (last_tp - begin_tp) / 1000.0);
			for (int i = 0; i < n; ++i) {
				out.printf("\t%d", m_counters.get(i).get());
			}
			out.println();
			out.flush();
		}
	}
	
	private String				m_outfile;
	private long				m_report_interval;
	private ArrayList<String>	m_desc;
	private ArrayList<AtomicLong> m_counters;
	private Thread				m_thread;
	private AtomicBoolean		m_stopped;
	private AtomicBoolean		m_need_to_stop;
}
