import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import jp.co.rakuten.rit.roma.client.AllTests;
import jp.co.rakuten.rit.roma.client.Node;
import jp.co.rakuten.rit.roma.client.RomaClient;
import jp.co.rakuten.rit.roma.client.RomaClientFactory;
import jp.co.rakuten.rit.roma.client.commands.TimeoutException;

public class PerfTestPutAndGet {
	private static String NODE_ID = AllTests.NODE_ID;

	private static RomaClient CLIENT = null;

	public static int LOOP_COUNT = 1000;

	public static int SIZE_OF_DATA = 1024;

	public static int NUM_OF_THREADS = 10;

	public static long PERIOD_OF_SLEEP = 1;

	public static long PERIOD_OF_TIMEOUT = 5000;

	private static final char A = 'b';

	private static final String DUMMY_PREFIX = makeDummyPrefix();

	private static String makeDummyPrefix() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < SIZE_OF_DATA; ++i) {
			sb.append(A);
		}
		sb.append("::");
		return sb.toString();
	}

	PerfTestPutAndGet() {
	}

	public void setUp() throws Exception {
		RomaClientFactory factory = RomaClientFactory.getInstance();
		List<Node> nodes = new ArrayList<Node>();
		nodes.add(Node.create(NODE_ID));
		CLIENT = factory.newRomaClient(new Properties());
		CLIENT.setNumOfThreads(NUM_OF_THREADS);
		CLIENT.setTimeout(PERIOD_OF_TIMEOUT);
		CLIENT.open(nodes);
	}

	public void tearDown() throws Exception {
		CLIENT.close();
		CLIENT = null;
	}

	public void testPutAndGet() throws Exception {
		Thread[] threads = new Thread[NUM_OF_THREADS];
		for (int i = 0; i < threads.length; ++i) {
			threads[i] = new Thread() {
				@Override
				public void run() {
					try {
						while (true) {
							doLoop();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};
		}
		for (int i = 0; i < threads.length; ++i) {
			threads[i].start();
		}
		while (true) {
			Thread.sleep(1000);
		}
	}

	private void doLoop() throws Exception {
		int count = 0;
		int count_threshold = 0;
		int count_threshold1 = 0;
		long count_max = 0;
		long count_min = 100000;
		long time0 = System.currentTimeMillis();
		while (count < LOOP_COUNT) {
			try {
				long time = System.currentTimeMillis();
				doLoop0();
				time = System.currentTimeMillis() - time;
				if (time > PERIOD_OF_TIMEOUT) {
					count_threshold++;
				}
				if (time > count_max) {
					count_max = time;
				}
				if (time < count_min) {
					count_min = time;
				}
			} catch (TimeoutException e) {
				count_threshold1++;
				System.out.println(e.getMessage());
				// e.printStackTrace();
			} catch (Exception e) {
				//e.printStackTrace();
				System.out.println(e.getMessage());
				// throw e;
			} finally {
				// Thread.sleep(PERIOD_OF_SLEEP);
				count++;
			}
		}
		time0 = System.currentTimeMillis() - time0;

		StringBuilder sb = new StringBuilder();
		sb.append("qps: ").append(
				(int) (((double) (LOOP_COUNT * 1000)) / time0)).append(
				" ").append("(timeout count: ").append(count_threshold).append(
				", ").append(count_threshold1).append(")").append(" max = ")
				.append(count_max / 1000).append(", min = ").append(
						count_min / 1000);
		System.out.println(sb.toString());
		count_min = 0;
		count_max = 0;
	}

	private void doLoop0() throws Exception {
		int queryPat = (int) (Math.random() * 10);
		int index = (int) (Math.random() * 10000);

		if (0 <= queryPat && queryPat <= 5) {
			CLIENT.put(new Integer(index).toString(), (DUMMY_PREFIX + index).getBytes());
		} else {
			CLIENT.get(new Integer(index).toString());
		}
	}

	public static void main(final String[] args) throws Exception {
		PerfTestPutAndGet test = new PerfTestPutAndGet();
		test.setUp();
		test.testPutAndGet();
		test.tearDown();
	}
}