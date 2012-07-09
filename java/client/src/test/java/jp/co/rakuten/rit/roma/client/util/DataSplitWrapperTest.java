package jp.co.rakuten.rit.roma.client.util;

import java.util.Properties;

import jp.co.rakuten.rit.roma.client.AllTests;
import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Node;
import jp.co.rakuten.rit.roma.client.RomaClient;
import jp.co.rakuten.rit.roma.client.RomaClientFactory;

import junit.framework.TestCase;

public class DataSplitWrapperTest extends TestCase {
  private static String NODE_ID = AllTests.NODE_ID;

  private static String KEY_PREFIX = DataSplitWrapper.class.getName();

  private static RomaClient CLIENT = null;

  private static DataSplitWrapper WRAPPER = null;

  private static String KEY = null;

  private static String largeData = null;

  private static String smallData = null;

  public DataSplitWrapperTest() {
    super();
    makeData();
  }

  private static void makeData() {
    int largeSize = 5120;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < largeSize; ++i) {
      sb.append("a");
    }
    largeData = sb.toString();
    sb = new StringBuilder();
    int smallSize = 512;
    for (int i = 0; i < smallSize; ++i) {
      sb.append("a");
    }
    smallData = sb.toString();
  }

  @Override
  public void setUp() throws Exception {
    RomaClientFactory factory = RomaClientFactory.getInstance();
    CLIENT = factory.newRomaClient(new Properties());
    WRAPPER = new DataSplitWrapper(CLIENT);
    CLIENT.open(Node.create(NODE_ID));
  }

  @Override
  public void tearDown() throws Exception {
    CLIENT.delete(KEY);
    CLIENT.close();
    CLIENT = null;
    WRAPPER = null;
    KEY = null;
  }

  public void testExpire0() throws Exception {
    try {
      KEY = KEY_PREFIX + "testExpire0";
      assertTrue(WRAPPER.put(KEY, largeData.getBytes(), 0));
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof ClientException);
    }
  }

  public void testPutAndGet01() throws Exception {
    try {
      KEY = KEY_PREFIX + "testPutAndGet01";
      assertTrue(WRAPPER.put(KEY, largeData.getBytes(), 10));
      byte[] b = WRAPPER.get(KEY);
      assertEquals(largeData, new String(b));
    } finally {
      WRAPPER.delete(KEY);
    }
  }

  public void testPutAndGet02() throws Exception {
    try {
      KEY = KEY_PREFIX + "testPutAndGet02";
      assertTrue(WRAPPER.put(KEY, smallData.getBytes(), 10));
      byte[] b = WRAPPER.get(KEY);
      assertEquals(smallData, new String(b));
    } finally {
      WRAPPER.delete(KEY);
    }
  }

  public void testPutAndGet03() throws Exception {
    try {
      KEY = KEY_PREFIX + "testPutAndGet03";
      assertTrue(CLIENT.put(KEY, largeData.getBytes(), 10));
      byte[] b = WRAPPER.get(KEY);
      assertEquals(largeData, new String(b));
    } finally {
      WRAPPER.delete(KEY);
    }
  }

  public void testPutAndGet04() throws Exception {
    try {
      KEY = KEY_PREFIX + "testPutAndGet04";
      assertTrue(CLIENT.put(KEY, smallData.getBytes(), 10));
      byte[] b = WRAPPER.get(KEY);
      assertEquals(smallData, new String(b));
    } finally {
      WRAPPER.delete(KEY);
    }
  }

  public void testPutAndGet05() throws Exception {
    try {
      KEY = KEY_PREFIX + "testPutAndGet05";
      assertTrue(CLIENT.put(KEY, largeData.getBytes(), 2));
      byte[] b = WRAPPER.get(KEY);
      assertEquals(largeData, new String(b));
      Thread.sleep(3000);
      b = WRAPPER.get(KEY);
      assertEquals(null, b);
    } finally {
      WRAPPER.delete(KEY);
    }
  }

  public void testPutAndGet06() throws Exception {
    try {
      KEY = KEY_PREFIX + "testPutAndGet06";
      assertTrue(CLIENT.put(KEY, smallData.getBytes(), 2));
      byte[] b = WRAPPER.get(KEY);
      assertEquals(smallData, new String(b));
      Thread.sleep(3000);
      b = WRAPPER.get(KEY);
      assertEquals(null, b);
    } finally {
      WRAPPER.delete(KEY);
    }
  }

  public void testPutAndDelete01() throws Exception {
    try {
      KEY = KEY_PREFIX + "testPutAndDelete01";
      assertTrue(WRAPPER.put(KEY, largeData.getBytes(), 10));
      byte[] b = WRAPPER.get(KEY);
      assertEquals(largeData, new String(b));
      assertTrue(WRAPPER.delete(KEY));
      b = WRAPPER.get(KEY);
      assertEquals(null, null);
    } finally {
      WRAPPER.delete(KEY);
    }
  }

  public void testPutAndDelete02() throws Exception {
    try {
      KEY = KEY_PREFIX + "testPutAndDelete02";
      assertTrue(WRAPPER.put(KEY, smallData.getBytes(), 10));
      byte[] b = WRAPPER.get(KEY);
      assertEquals(smallData, new String(b));
      assertTrue(WRAPPER.delete(KEY));
      b = WRAPPER.get(KEY);
      assertEquals(null, null);
    } finally {
      WRAPPER.delete(KEY);
    }
  }

  public void testPutAndDelete03() throws Exception {
    try {
      KEY = KEY_PREFIX + "testPutAndDelete03";
      assertTrue(CLIENT.put(KEY, largeData.getBytes(), 10));
      byte[] b = WRAPPER.get(KEY);
      assertEquals(largeData, new String(b));
      assertTrue(WRAPPER.delete(KEY));
      b = WRAPPER.get(KEY);
      assertEquals(null, null);
    } finally {
      WRAPPER.delete(KEY);
    }
  }

  public void testPutAndDelete04() throws Exception {
    try {
      KEY = KEY_PREFIX + "testPutAndDelete04";
      assertTrue(CLIENT.put(KEY, smallData.getBytes(), 10));
      byte[] b = WRAPPER.get(KEY);
      assertEquals(smallData, new String(b));
      assertTrue(WRAPPER.delete(KEY));
      b = WRAPPER.get(KEY);
      assertEquals(null, null);
    } finally {
      WRAPPER.delete(KEY);
    }
  }
}