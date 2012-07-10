package jp.co.rakuten.rit.roma.client.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import jp.co.rakuten.rit.roma.client.AllTests;
import jp.co.rakuten.rit.roma.client.Node;
import jp.co.rakuten.rit.roma.client.RomaClient;
import jp.co.rakuten.rit.roma.client.RomaClientFactory;
import junit.framework.TestCase;

public class MapcountWrapperTest extends TestCase {
  private static String NODE_ID = AllTests.NODE_ID;

  private static String KEY_PREFIX = MapcountWrapperTest.class.getName();

  private static RomaClient CLIENT = null;

  private static MapcountWrapper MAPCOUNTUTIL = null;

  private static String KEY = null;

  private static Map<String, Integer> KEYS = null;

  private static HashMap<String, Object> RESULT = null;

  public MapcountWrapperTest() {
    super();
  }

  @Override
  public void setUp() throws Exception {
    RomaClientFactory factory = RomaClientFactory.getInstance();
    CLIENT = factory.newRomaClient(new Properties());
    MAPCOUNTUTIL = new MapcountWrapper(CLIENT);
    CLIENT.open(Node.create(NODE_ID));
  }

  @Override
  public void tearDown() throws Exception {
    MAPCOUNTUTIL = null;
    CLIENT.delete(KEY);
    CLIENT.close();
    CLIENT = null;
    KEY = null;
  }

  @SuppressWarnings("unchecked")
  public void testCountup01() throws Exception {
    KEY = KEY_PREFIX + "testCountup01";
    KEYS = new HashMap<String, Integer>();
    KEYS.put("a", 1);
    KEYS.put("bb", 1);
    KEYS.put("ccc", 1);
    RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.countup(KEY, KEYS, 0);
    assertEquals(4, RESULT.size());
    assertEquals(new BigDecimal("1"), RESULT.get("a"));
    assertEquals(new BigDecimal("1"), RESULT.get("bb"));
    assertEquals(new BigDecimal("1"), RESULT.get("ccc"));
  }

  @SuppressWarnings("unchecked")
  public void testCountup02() throws Exception {
    KEY = KEY_PREFIX + "testCountup02";
    KEYS = new HashMap<String, Integer>();
    KEYS.put("a", 1);
    for (int i = 0; i < 100; i++) {
      RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.countup(KEY, KEYS, 0);
    }
    assertEquals(2, RESULT.size());
    assertEquals(new BigDecimal("100"), RESULT.get("a"));
  }

  @SuppressWarnings("unchecked")
  public void testCountup03() throws Exception {
    KEY = KEY_PREFIX + "testCountup03";
    KEYS = new HashMap<String, Integer>();
    KEYS.put("a", 3);
    RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.countup(KEY, KEYS, 0);
    assertEquals(2, RESULT.size());
    assertEquals(new BigDecimal("3"), RESULT.get("a"));
  }

  @SuppressWarnings("unchecked")
  public void testCountup04() throws Exception {
    KEY = KEY_PREFIX + "testCountup04";
    KEYS = new HashMap<String, Integer>();
    KEYS.put("a", 1);
    KEYS.put("bb", 1);
    KEYS.put("ccc", 1);
    RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.countup(KEY, KEYS);
    assertEquals(4, RESULT.size());
    assertEquals(new BigDecimal("1"), RESULT.get("a"));
    assertEquals(new BigDecimal("1"), RESULT.get("bb"));
    assertEquals(new BigDecimal("1"), RESULT.get("ccc"));
  }

  @SuppressWarnings("unchecked")
  public void testCountup05() throws Exception {
    KEY = KEY_PREFIX + "testCountup02";
    KEYS = new HashMap<String, Integer>();
    KEYS.put("a", 1);
    for (int i = 0; i < 100; i++) {
      RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.countup(KEY, KEYS);
    }
    assertEquals(2, RESULT.size());
    assertEquals(new BigDecimal("100"), RESULT.get("a"));
  }

  @SuppressWarnings("unchecked")
  public void testCountup06() throws Exception {
    KEY = KEY_PREFIX + "testCountup03";
    KEYS = new HashMap<String, Integer>();
    KEYS.put("a", 3);
    RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.countup(KEY, KEYS);
    assertEquals(2, RESULT.size());
    assertEquals(new BigDecimal("3"), RESULT.get("a"));
  }

  @SuppressWarnings("unchecked")
  public void testCountup07() throws Exception {
    KEY = KEY_PREFIX + "testCountup07";
    KEYS = new HashMap<String, Integer>();
    KEYS.put("a", 1);
    RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.countup(KEY, KEYS, 1);
    Thread.sleep(2000);
    RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.get(KEY);
    assertNull(RESULT);
  }

  @SuppressWarnings("unchecked")
  public void testGet01() throws Exception {
    KEY = KEY_PREFIX + "testGet01";

    KEYS = new HashMap<String, Integer>();
    KEYS.put("a", 1);
    KEYS.put("b", 2);
    KEYS.put("c", 3);

    List subKeys = new ArrayList<String>();
    subKeys.add("a");
    subKeys.add("b");
    subKeys.add("c");

    RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.countup(KEY, KEYS, 0);
    String lt = (String)RESULT.get("last_updated_date");

    RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.get(KEY);
    assertEquals(lt, (String)RESULT.get("last_updated_date"));
    assertEquals(new BigDecimal("1"), RESULT.get("a"));
    assertEquals(new BigDecimal("2"), RESULT.get("b"));
    assertEquals(new BigDecimal("3"), RESULT.get("c"));

    RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.get(KEY, subKeys);
    assertEquals(lt, (String)RESULT.get("last_updated_date"));
    assertEquals(new BigDecimal("1"), RESULT.get("a"));
    assertEquals(new BigDecimal("2"), RESULT.get("b"));
    assertEquals(new BigDecimal("3"), RESULT.get("c"));
  }

  @SuppressWarnings("unchecked")
  public void testGet02() throws Exception {
    KEY = KEY_PREFIX + "testGet02";
    List subKeys = new ArrayList<String>();
    subKeys.add("a");
    RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.get(KEY);
    assertNull(RESULT);
    RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.get(KEY, subKeys);
    assertNull(RESULT);
  }

  @SuppressWarnings("unchecked")
  public void testUpdate01() throws Exception {
    KEY = KEY_PREFIX + "testUpdate01";
    KEYS = new HashMap<String, Integer>();
    KEYS.put("a", 1);
    List subKeys = new ArrayList<String>();
    subKeys.add("a");

    RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.countup(KEY, KEYS);
    String lt = (String)RESULT.get("last_updated_date");

    RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.update(KEY);
    assertNotSame(lt, (String)RESULT.get("last_updated_date"));
    assertEquals(new BigDecimal("1"), RESULT.get("a"));

    RESULT = (HashMap<String, Object>) MAPCOUNTUTIL.update(KEY, subKeys);
    assertNotSame(lt, (String)RESULT.get("last_updated_date"));
    assertEquals(new BigDecimal("1"), RESULT.get("a"));
    }
}
