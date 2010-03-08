package jp.co.rakuten.rit.roma.client;

import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import jp.co.rakuten.rit.roma.client.commands.TimeoutFilter;
import junit.framework.TestCase;

public class RomaClientImplTest extends TestCase {

	private static String NODE_ID = AllTests.NODE_ID;

	private static String KEY_PREFIX = RomaClientImplTest.class.getName();

	private static RomaClient CLIENT = null;

	private static String KEY = null;

	public RomaClientImplTest() {
		super();
	}

	@Override
	public void setUp() throws Exception {
		RomaClientFactory factory = RomaClientFactory.getInstance();
		CLIENT = factory.newRomaClient(new Properties());
		CLIENT.open(Node.create(NODE_ID));
		TimeoutFilter.timeout = 100 * 1000;
	}

	@Override
	public void tearDown() throws Exception {
		CLIENT.delete(KEY);
		CLIENT.close();
		CLIENT = null;
		KEY = null;
	}

	public void testGets01() throws Exception {
		try {
			KEY = KEY_PREFIX + "testGets01";
			assertTrue(CLIENT.put(KEY + "01", "01".getBytes()));
			assertTrue(CLIENT.put(KEY + "02", "02".getBytes()));
			assertTrue(CLIENT.put(KEY + "03", "03".getBytes()));
			List<String> keys = new ArrayList<String>();
			keys.add(KEY + "01");
			keys.add(KEY + "02");
			keys.add(KEY + "03");
			Map<String, byte[]> values = CLIENT.gets(keys);
			assertEquals(3, values.size());
			assertEquals("01", new String(values.get(KEY + "01")));
			assertEquals("02", new String(values.get(KEY + "02")));
			assertEquals("03", new String(values.get(KEY + "03")));
		} finally {
			CLIENT.delete(KEY + "01");
			CLIENT.delete(KEY + "02");
			CLIENT.delete(KEY + "03");
		}
	}

	public void testGets02() throws Exception {
		try {
			KEY = KEY_PREFIX + "testGets02";
			assertTrue(CLIENT.put(KEY + "01", "01".getBytes()));
			assertTrue(CLIENT.put(KEY + "02", "02".getBytes()));
			assertTrue(CLIENT.put(KEY + "03", "03".getBytes()));
			List<String> keys = new ArrayList<String>();
			keys.add(KEY + "01");
			keys.add(KEY + "02");
			keys.add(KEY + "04");
			keys.add(KEY + "05");
			keys.add(KEY + "03");
			Map<String, byte[]> values = CLIENT.gets(keys);
			assertEquals(3, values.size());
			assertEquals("01", new String(values.get(KEY + "01")));
			assertEquals("02", new String(values.get(KEY + "02")));
			assertEquals("03", new String(values.get(KEY + "03")));
		} finally {
			CLIENT.delete(KEY + "01");
			CLIENT.delete(KEY + "02");
			CLIENT.delete(KEY + "03");
		}
	}

	public void testGets03() throws Exception {
		try {
			KEY = KEY_PREFIX + "testGets03";
			assertTrue(CLIENT.put(KEY + "01", "01".getBytes(), new Date(2000)));
			assertTrue(CLIENT.put(KEY + "02", "02".getBytes()));
			assertTrue(CLIENT.put(KEY + "03", "03".getBytes()));
			List<String> keys = new ArrayList<String>();
			keys.add(KEY + "01");
			keys.add(KEY + "02");
			keys.add(KEY + "04");
			keys.add(KEY + "05");
			keys.add(KEY + "03");
			Map<String, byte[]> values = CLIENT.gets(keys);
			assertEquals(3, values.size());
			assertEquals("01", new String(values.get(KEY + "01")));
			assertEquals("02", new String(values.get(KEY + "02")));
			assertEquals("03", new String(values.get(KEY + "03")));
			Thread.sleep(3000);
			values = CLIENT.gets(keys);
			assertEquals(2, values.size());
			assertEquals("02", new String(values.get(KEY + "02")));
			assertEquals("03", new String(values.get(KEY + "03")));
		} finally {
			CLIENT.delete(KEY + "01");
			CLIENT.delete(KEY + "02");
			CLIENT.delete(KEY + "03");
		}
	}
	
    public void testGets04() throws Exception {
        try {
            KEY = KEY_PREFIX + "testGets04";
            assertTrue(CLIENT.put(KEY + "01", "01".getBytes()));
            assertTrue(CLIENT.put(KEY + "02", "02".getBytes()));
            assertTrue(CLIENT.put(KEY + "03", "03".getBytes()));
            List<String> keys = new ArrayList<String>();
            keys.add(KEY + "01");
            keys.add(KEY + "02");
            keys.add(KEY + "03");
            Map<String, byte[]> values = CLIENT.gets(keys, true);
            assertEquals(3, values.size());
            assertEquals("01", new String(values.get(KEY + "01")));
            assertEquals("02", new String(values.get(KEY + "02")));
            assertEquals("03", new String(values.get(KEY + "03")));
        } finally {
            CLIENT.delete(KEY + "01");
            CLIENT.delete(KEY + "02");
            CLIENT.delete(KEY + "03");
        }
    }
    
    public void testGets05() throws Exception {
        try {
            KEY = KEY_PREFIX + "testGets05";
            assertTrue(CLIENT.put(KEY + "01", "01".getBytes()));
            assertTrue(CLIENT.put(KEY + "02", "02".getBytes()));
            assertTrue(CLIENT.put(KEY + "03", "03".getBytes()));
            List<String> keys = new ArrayList<String>();
            keys.add(KEY + "01");
            keys.add(KEY + "02");
            keys.add(KEY + "04");
            keys.add(KEY + "05");
            keys.add(KEY + "03");
            Map<String, byte[]> values = CLIENT.gets(keys);
            assertEquals(3, values.size());
            assertEquals("01", new String(values.get(KEY + "01")));
            assertEquals("02", new String(values.get(KEY + "02")));
            assertEquals("03", new String(values.get(KEY + "03")));
        } finally {
            CLIENT.delete(KEY + "01");
            CLIENT.delete(KEY + "02");
            CLIENT.delete(KEY + "03");
        }
    }

    public void testGets06() throws Exception {
        try {
            KEY = KEY_PREFIX + "testGets06";
            assertTrue(CLIENT.put(KEY + "01", "01".getBytes(), new Date(2000)));
            assertTrue(CLIENT.put(KEY + "02", "02".getBytes()));
            assertTrue(CLIENT.put(KEY + "03", "03".getBytes()));
            List<String> keys = new ArrayList<String>();
            keys.add(KEY + "01");
            keys.add(KEY + "02");
            keys.add(KEY + "04");
            keys.add(KEY + "05");
            keys.add(KEY + "03");
            Map<String, byte[]> values = CLIENT.gets(keys);
            assertEquals(3, values.size());
            assertEquals("01", new String(values.get(KEY + "01")));
            assertEquals("02", new String(values.get(KEY + "02")));
            assertEquals("03", new String(values.get(KEY + "03")));
            Thread.sleep(3000);
            values = CLIENT.gets(keys);
            assertEquals(2, values.size());
            assertEquals("02", new String(values.get(KEY + "02")));
            assertEquals("03", new String(values.get(KEY + "03")));
        } finally {
            CLIENT.delete(KEY + "01");
            CLIENT.delete(KEY + "02");
            CLIENT.delete(KEY + "03");
        }
    }
    
	public void testPut01() throws Exception {
		KEY = KEY_PREFIX + "testPut01";
		assertTrue(CLIENT.put(KEY, "01".getBytes()));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("01", new String(ret));
	}

	public void testPut02() throws Exception {
		KEY = KEY_PREFIX + "testPut02";
		Date zero = new Date(0);
		assertTrue(CLIENT.put(KEY, "01".getBytes(), zero));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("01", new String(ret));
	}

	public void testPut03() throws Exception {
		KEY = KEY_PREFIX + "testPut03";
		Date one = new Date(2000);
		assertTrue(CLIENT.put(KEY, "01".getBytes(), one));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("01", new String(ret));
		Thread.sleep(3000);
		ret = CLIENT.get(KEY);
		assertEquals(null, ret);
	}

	public void testAppend01() throws Exception {
		KEY = KEY_PREFIX + "testAppend01";
		assertTrue(CLIENT.put(KEY, "01".getBytes()));
		assertTrue(CLIENT.append(KEY, "02".getBytes()));
		assertTrue(CLIENT.append(KEY, "03".getBytes()));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("010203", new String(ret));
	}

	public void testAppend02() throws Exception {
		KEY = KEY_PREFIX + "testAppend02";
		assertFalse(CLIENT.append(KEY, "01".getBytes()));
	}

	public void testAppend03() throws Exception {
		KEY = KEY_PREFIX + "testAppend03";
		assertTrue(CLIENT.put(KEY, "01".getBytes()));
		assertTrue(CLIENT.append(KEY, "02".getBytes()));
		assertTrue(CLIENT.append(KEY, "03".getBytes(), new Date(2000)));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("010203", new String(ret));
		Thread.sleep(3000);
		ret = CLIENT.get(KEY);
		assertEquals(null, ret);
	}

	public void testAppend04() throws Exception {
		KEY = KEY_PREFIX + "testAppend04";
		assertTrue(CLIENT.put(KEY, "01".getBytes()));
		assertTrue(CLIENT.append(KEY, "02".getBytes()));
		assertTrue(CLIENT.append(KEY, "03".getBytes(), new Date(2000)));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("010203", new String(ret));
		Thread.sleep(3000);
		assertFalse(CLIENT.append(KEY, "04".getBytes()));
	}

	public void testPrepend01() throws Exception {
		KEY = KEY_PREFIX + "testPrepend01";
		assertTrue(CLIENT.put(KEY, "01".getBytes()));
		assertTrue(CLIENT.prepend(KEY, "02".getBytes()));
		assertTrue(CLIENT.prepend(KEY, "03".getBytes()));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("030201", new String(ret));
	}

	public void testPrepend02() throws Exception {
		KEY = KEY_PREFIX + "testPrepend02";
		assertFalse(CLIENT.prepend(KEY, "01".getBytes()));
	}

	public void testPrepend03() throws Exception {
		KEY = KEY_PREFIX + "testPrepend03";
		assertTrue(CLIENT.put(KEY, "01".getBytes()));
		assertTrue(CLIENT.prepend(KEY, "02".getBytes()));
		assertTrue(CLIENT.prepend(KEY, "03".getBytes(), new Date(2000)));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("030201", new String(ret));
		Thread.sleep(3000);
		ret = CLIENT.get(KEY);
		assertEquals(null, ret);
	}

	public void testPrepend04() throws Exception {
		KEY = KEY_PREFIX + "testPrepend04";
		assertTrue(CLIENT.put(KEY, "01".getBytes()));
		assertTrue(CLIENT.prepend(KEY, "02".getBytes()));
		assertTrue(CLIENT.prepend(KEY, "03".getBytes(), new Date(2000)));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("030201", new String(ret));
		Thread.sleep(3000);
		assertFalse(CLIENT.prepend(KEY, "04".getBytes()));
	}
	
    public void testAdd01() throws Exception {
        try {
            KEY = KEY_PREFIX + "testAdd01";
            assertTrue(CLIENT.add(KEY + "01", "01".getBytes()));
            assertFalse(CLIENT.add(KEY + "01", "02".getBytes()));
            List<String> keys = new ArrayList<String>();
            keys.add(KEY + "01");
            Map<String, byte[]> values = CLIENT.gets(keys);
            assertEquals(1, values.size());
            assertEquals("01", new String(values.get(KEY + "01")));
        } finally {
            CLIENT.delete(KEY + "01");
        }
    }

    public void testGetsAndCas01() throws Exception {
        try {
            KEY = KEY_PREFIX + "testGetsAndCas01";
            assertTrue(CLIENT.put(KEY + "01", "01".getBytes()));
            assertTrue(CLIENT.put(KEY + "02", "02".getBytes()));
            assertTrue(CLIENT.put(KEY + "03", "03".getBytes()));
            List<String> keys = new ArrayList<String>();
            keys.add(KEY + "01");
            keys.add(KEY + "02");
            keys.add(KEY + "03");
            Map<String, CasValue> values = CLIENT.getsWithCasID(keys);
            assertEquals(3, values.size());
            assertEquals("01", new String(values.get(KEY + "01").getValue()));
            assertEquals("02", new String(values.get(KEY + "02").getValue()));
            assertEquals("03", new String(values.get(KEY + "03").getValue()));
            
            assertEquals(CasResponse.OK, CLIENT.cas(KEY + "01", values.get(KEY + "01").getCas(), "001".getBytes()));
            assertEquals(CasResponse.NOT_FOUND, CLIENT.cas(KEY + "04", values.get(KEY + "01").getCas(), "001".getBytes()));
            assertEquals(CasResponse.EXISTS, CLIENT.cas(KEY + "01", values.get(KEY + "01").getCas(), "0001".getBytes()));
        } finally {
            CLIENT.delete(KEY + "01");
            CLIENT.delete(KEY + "02");
            CLIENT.delete(KEY + "03");
        }
    }

    public void testHashName() throws Exception {
        KEY = KEY_PREFIX + "testHashName";
        String hashName = "another.hash";
        createHash(hashName);
        RomaClient another = null;
        try {
            RomaClientFactory factory = RomaClientFactory.getInstance();
            Properties prop = new Properties();
            prop.put(Config.HASH_NAME, hashName);
            another = factory.newRomaClient(prop);
            another.open(Node.create(NODE_ID));
            assertEquals(hashName, another.getHashName());

            // put & get
            assertTrue(CLIENT.put(KEY, "01".getBytes()));
            assertTrue(another.put(KEY, "02".getBytes()));
            assertEquals("01", new String(CLIENT.get(KEY)));
            assertEquals("02", new String(another.get(KEY)));

            // append & prepend
            assertTrue(CLIENT.append(KEY, "02".getBytes()));
            assertTrue(CLIENT.prepend(KEY, "03".getBytes()));
            assertTrue(another.append(KEY, "04".getBytes()));
            assertTrue(another.prepend(KEY, "05".getBytes()));
            assertEquals("030102", new String(CLIENT.get(KEY)));
            assertEquals("050204", new String(another.get(KEY)));

            // add
            assertTrue(CLIENT.add(KEY + "01", "01".getBytes()));
            assertTrue(another.add(KEY + "01", "02".getBytes()));
            assertEquals("01", new String(CLIENT.get(KEY + "01")));
            assertEquals("02", new String(another.get(KEY + "01")));

            // gets
            List<String> keys = new ArrayList<String>();
            keys.add(KEY);
            keys.add(KEY + "01");
            Map<String, byte[]> values1 = CLIENT.gets(keys);
            Map<String, byte[]> values2 = another.gets(keys);
            assertEquals(2, values1.size());
            assertEquals(2, values2.size());
            assertEquals("030102", new String(values1.get(KEY)));
            assertEquals("050204", new String(values2.get(KEY)));
            assertEquals("01", new String(values1.get(KEY + "01")));
            assertEquals("02", new String(values2.get(KEY + "01")));

            values1 = CLIENT.gets(keys, true);
            values2 = another.gets(keys, true);
            assertEquals(2, values1.size());
            assertEquals(2, values2.size());
            assertEquals("030102", new String(values1.get(KEY)));
            assertEquals("050204", new String(values2.get(KEY)));
            assertEquals("01", new String(values1.get(KEY + "01")));
            assertEquals("02", new String(values2.get(KEY + "01")));

            // gets & cas
            Map<String, CasValue> values3 = CLIENT.getsWithCasID(keys);
            Map<String, CasValue> values4 = another.getsWithCasID(keys);
            assertEquals(2, values3.size());
            assertEquals(2, values4.size());
            assertEquals("030102", new String(values3.get(KEY).getValue()));
            assertEquals("050204", new String(values4.get(KEY).getValue()));
            assertEquals("01", new String(values3.get(KEY + "01").getValue()));
            assertEquals("02", new String(values4.get(KEY + "01").getValue()));

            values3 = CLIENT.getsWithCasID(keys, true);
            values4 = another.getsWithCasID(keys, true);
            assertEquals(2, values3.size());
            assertEquals(2, values4.size());
            assertEquals("030102", new String(values3.get(KEY).getValue()));
            assertEquals("050204", new String(values4.get(KEY).getValue()));
            assertEquals("01", new String(values3.get(KEY + "01").getValue()));
            assertEquals("02", new String(values4.get(KEY + "01").getValue()));

            assertEquals(CasResponse.OK, CLIENT.cas(KEY + "01", values3.get(KEY + "01").getCas(), "001".getBytes()));
            assertEquals(CasResponse.OK, another.cas(KEY + "01", values4.get(KEY + "01").getCas(), "002".getBytes()));
            assertEquals("001", new String(CLIENT.get(KEY + "01")));
            assertEquals("002", new String(another.get(KEY + "01")));

            // incr & decr
            assertTrue(CLIENT.put(KEY + "02", "0".getBytes()));
            assertTrue(another.put(KEY + "02", "0".getBytes()));
            assertEquals(2, CLIENT.incr(KEY + "02", 2).intValue());
            assertEquals(5, another.incr(KEY + "02", 5).intValue());
            assertEquals(1, CLIENT.decr(KEY + "02", 1).intValue());
            assertEquals(3, another.decr(KEY + "02", 2).intValue());

            // delete
            assertTrue(another.delete(KEY + "01"));
            assertNull(another.get(KEY + "01"));
            assertEquals("001", new String(CLIENT.get(KEY + "01")));

        } finally {
            CLIENT.delete(KEY + "01");
            CLIENT.delete(KEY + "02");
            if (another != null) {
                another.close();
            }
            deleteHash(hashName);
        }
    }

    private void createHash(String hashName) throws Exception {
        Node node = Node.create(NODE_ID);
        Socket sock = new Socket(node.getHost(), node.getPort());
        Connection conn = new Connection(sock);
        try {
            StringBuilder sb =  new StringBuilder();
            sb.append("createhash ").append(hashName).append("\r\n");
            conn.out.write(sb.toString().getBytes());
            conn.out.flush();
            String response = conn.in.readLine();
            if (response.indexOf("\"" + NODE_ID +"\"=>\"CREATED\"") == -1) {
                throw new ClientException(response);
            }
        } finally {
            conn.close();
        }
    }

    private void deleteHash(String hashName) throws Exception {
        Node node = Node.create(NODE_ID);
        Socket sock = new Socket(node.getHost(), node.getPort());
        Connection conn = new Connection(sock);
        try {
            StringBuilder sb =  new StringBuilder();
            sb.append("deletehash ").append(hashName).append("\r\n");
            conn.out.write(sb.toString().getBytes());
            conn.out.flush();
            String response = conn.in.readLine();
            if (response.indexOf("\"" + NODE_ID +"\"=>\"DELETED\"") == -1) {
                throw new ClientException(response);
            }
        } finally {
            conn.close();
        }
    }
}
