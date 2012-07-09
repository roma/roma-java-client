package jp.co.rakuten.rit.roma.client.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.CommandFactory;
import jp.co.rakuten.rit.roma.client.RomaClient;
import jp.co.rakuten.rit.roma.client.commands.Command;
import jp.co.rakuten.rit.roma.client.commands.CommandContext;
import jp.co.rakuten.rit.roma.client.commands.FailOverFilter;
import jp.co.rakuten.rit.roma.client.commands.TimeoutFilter;
import jp.co.rakuten.rit.roma.client.util.commands.ClearCommand;
import jp.co.rakuten.rit.roma.client.util.commands.DeleteAtCommand;
import jp.co.rakuten.rit.roma.client.util.commands.DeleteCommand;
import jp.co.rakuten.rit.roma.client.util.commands.ExpiredSwapAndInsertCommand;
import jp.co.rakuten.rit.roma.client.util.commands.ExpiredSwapAndPushCommand;
import jp.co.rakuten.rit.roma.client.util.commands.ExpiredSwapAndSizedInsertCommand;
import jp.co.rakuten.rit.roma.client.util.commands.ExpiredSwapAndSizedPushCommand;
import jp.co.rakuten.rit.roma.client.util.commands.GetsCommand;
import jp.co.rakuten.rit.roma.client.util.commands.GetsWithTimeCommand;
import jp.co.rakuten.rit.roma.client.util.commands.InsertCommand;
import jp.co.rakuten.rit.roma.client.util.commands.JoinCommand;
import jp.co.rakuten.rit.roma.client.util.commands.JoinWithTimeCommand;
import jp.co.rakuten.rit.roma.client.util.commands.LengthCommand;
import jp.co.rakuten.rit.roma.client.util.commands.ListCommandID;
import jp.co.rakuten.rit.roma.client.util.commands.PushCommand;
import jp.co.rakuten.rit.roma.client.util.commands.SizedInsertCommand;
import jp.co.rakuten.rit.roma.client.util.commands.SizedPushCommand;
import jp.co.rakuten.rit.roma.client.util.commands.SwapAndInsertCommand;
import jp.co.rakuten.rit.roma.client.util.commands.SwapAndPushCommand;
import jp.co.rakuten.rit.roma.client.util.commands.SwapAndSizedInsertCommand;
import jp.co.rakuten.rit.roma.client.util.commands.SwapAndSizedPushCommand;
import jp.co.rakuten.rit.roma.client.util.commands.UpdateCommand;

/**
 * 
 * Sample code might look like:
 * 
 * <blockquote>
 * 
 * <pre>
 * public static void main(String[] args) throws Exception {
 *   List&lt;Node&gt; initNodes = new ArrayList&lt;Node&gt;();
 *   initNodes.add(Node.create(&quot;localhost_11211&quot;));
 *   initNodes.add(Node.create(&quot;localhost_11212&quot;));
 * 
 *   // create and initialize the instance of ROMA client
 *   RomaClientFactory fact = RomaClientFactory.getInstance();
 *   RomaClient client = fact.newRomaClient();
 * 
 *   // create and initialize ROMA client's wrapper for List API
 *   ListWrapper listWrapper = new ListWrapper(client, 3);
 *   List&lt;byte[]&gt; ret = null;
 *   String s = null;
 * 
 *   // open connections with ROMA
 *   client.open(initNodes);
 * 
 *   // prepend
 *   listWrapper.prepend(&quot;muga&quot;, &quot;v1&quot;.getBytes());
 *   listWrapper.prepend(&quot;muga&quot;, &quot;v2&quot;.getBytes());
 *   listWrapper.prepend(&quot;muga&quot;, &quot;v3&quot;.getBytes());
 *   listWrapper.prepend(&quot;muga&quot;, &quot;v4&quot;.getBytes());
 *   ret = listWrapper.get(&quot;muga&quot;);
 *   s = new String(ret.get(0)); // s is &quot;v4&quot;
 *   s = new String(ret.get(1)); // s is &quot;v3&quot;
 *   s = new String(ret.get(2)); // s is &quot;v2&quot;
 * 
 *   // deleteAndPrepend 
 *   listWrapper.deleteAndPrepend(&quot;muga&quot;, v2);
 *   ret = listWrapper.get(&quot;muga&quot;);
 *   s = new String(ret.get(0)); // s is &quot;v2&quot;
 *   s = new String(ret.get(1)); // s is &quot;v4&quot;
 *   s = new String(ret.get(2)); // s is &quot;v3&quot;
 * 
 *   // close the connection
 *   client.close();
 * }
 * </pre>
 * 
 * </blockquote>
 * 
 * 
 */
public class ListWrapper {
  private static Logger LOG = LoggerFactory.getLogger(ListWrapper.class);

  public static class Entry {

    private byte[] value;
    private long time;

    Entry(byte[] value, String time) {
      this.value = value;
      this.time = Long.parseLong(time);
    }

    public byte[] getValue() {
      return value;
    }

    public long getTime() {
      return time;
    }

    @Override
    public String toString() {
      return new String(value) + "_" + time;
    }
  }

  protected RomaClient client;
  protected int listSize = 0;
  protected long expiry = 0;

  public ListWrapper(RomaClient client) throws ClientException {
    this(client, 0);
  }

  public ListWrapper(RomaClient client, int listSize) throws ClientException {
    this(client, listSize, 0);
  }

  public ListWrapper(RomaClient client, long expiry) throws ClientException {
    this(client, 0, expiry);
  }

  public ListWrapper(RomaClient client, int listSize, long expiry)
      throws ClientException {
    this.client = client;
    setListSize(listSize);
    setExpiry(expiry);
    CommandFactory commandFact = client.getCommandFactory();
    commandFact.createCommand(ListCommandID.ALIST_PUSH, new FailOverFilter(new TimeoutFilter(new PushCommand())));
    commandFact.createCommand(ListCommandID.ALIST_SIZED_PUSH, new FailOverFilter(new TimeoutFilter(new SizedPushCommand())));
    commandFact.createCommand(ListCommandID.ALIST_SWAP_AND_PUSH, new FailOverFilter(new TimeoutFilter(new SwapAndPushCommand())));
    commandFact.createCommand(ListCommandID.ALIST_SWAP_AND_SIZED_PUSH, new FailOverFilter(new TimeoutFilter(new SwapAndSizedPushCommand())));
    commandFact.createCommand(ListCommandID.ALIST_EXPIRED_SWAP_AND_PUSH, new FailOverFilter(new TimeoutFilter(new ExpiredSwapAndPushCommand())));
    commandFact.createCommand(ListCommandID.ALIST_EXPIRED_SWAP_AND_SIZED_PUSH, new FailOverFilter(new TimeoutFilter(new ExpiredSwapAndSizedPushCommand())));
    commandFact.createCommand(ListCommandID.ALIST_DELETE_AT, new FailOverFilter(new TimeoutFilter(new DeleteAtCommand())));
    commandFact.createCommand(ListCommandID.ALIST_DELETE, new FailOverFilter(new TimeoutFilter(new DeleteCommand())));
    commandFact.createCommand(ListCommandID.ALIST_INSERT, new FailOverFilter(new TimeoutFilter(new InsertCommand())));
    commandFact.createCommand(ListCommandID.ALIST_SIZED_INSERT, new FailOverFilter(new TimeoutFilter(new SizedInsertCommand())));
    commandFact.createCommand(ListCommandID.ALIST_SWAP_AND_INSERT, new FailOverFilter(new TimeoutFilter(new SwapAndInsertCommand())));
    commandFact.createCommand(ListCommandID.ALIST_SWAP_AND_SIZED_INSERT, new FailOverFilter(new TimeoutFilter(new SwapAndSizedInsertCommand())));
    commandFact.createCommand(ListCommandID.ALIST_EXPIRED_SWAP_AND_INSERT, new FailOverFilter(new TimeoutFilter(new ExpiredSwapAndInsertCommand())));
    commandFact.createCommand(ListCommandID.ALIST_EXPIRED_SWAP_AND_SIZED_INSERT, new FailOverFilter(new TimeoutFilter(new ExpiredSwapAndSizedInsertCommand())));
    commandFact.createCommand(ListCommandID.ALIST_JOIN, new FailOverFilter(new TimeoutFilter(new JoinCommand())));
    commandFact.createCommand(ListCommandID.ALIST_JOIN_WITH_TIME, new FailOverFilter(new TimeoutFilter(new JoinWithTimeCommand())));
    commandFact.createCommand(ListCommandID.ALIST_GETS, new FailOverFilter(new TimeoutFilter(new GetsCommand())));
    commandFact.createCommand(ListCommandID.ALIST_GETS_WITH_TIME, new FailOverFilter(new TimeoutFilter(new GetsWithTimeCommand())));
    commandFact.createCommand(ListCommandID.ALIST_CLEAR, new FailOverFilter(new TimeoutFilter(new ClearCommand())));
    commandFact.createCommand(ListCommandID.ALIST_LENGTH, new FailOverFilter(new TimeoutFilter(new LengthCommand())));
  }

  public void setListSize(int listSize) {
    if (listSize < 0) {
      throw new IllegalArgumentException();
    }
    this.listSize = listSize;
  }

  public int getListSize() {
    return listSize;
  }

  public void setExpiry(long expiry) {
    if (expiry < 0) {
      throw new IllegalArgumentException();
    }
    this.expiry = expiry;
  }

  public long getExpiry() {
    return expiry;
  }

  public boolean append(String key, byte[] value) throws ClientException {
    if (getListSize() == 0) {
      return updateList(ListCommandID.ALIST_PUSH, key, value);
    } else {
      return updateList(ListCommandID.ALIST_SIZED_PUSH, key, value);
    }
  }

  public boolean delete(String key, int index) throws ClientException {
    return updateList(ListCommandID.ALIST_DELETE_AT, key, (new Integer(index))
        .toString().getBytes());
  }

  public boolean delete(String key, byte[] value) throws ClientException {
    return updateList(ListCommandID.ALIST_DELETE, key, value);
  }

  public boolean deleteAndAppend(String key, byte[] value)
      throws ClientException {
    int size = getListSize();
    long expiry = getExpiry();
    if (size == 0 && expiry == 0) {
      return updateList(ListCommandID.ALIST_SWAP_AND_PUSH, key, value);
    } else if (size == 0 && expiry != 0) {
      return updateList(ListCommandID.ALIST_EXPIRED_SWAP_AND_PUSH, key, value);
    } else if (size != 0 && expiry == 0) {
      return updateList(ListCommandID.ALIST_SWAP_AND_SIZED_PUSH, key, value);
    } else { // size != 0 && expiry != 0
      return updateList(ListCommandID.ALIST_EXPIRED_SWAP_AND_SIZED_PUSH, key,
          value);
    }
  }

  public boolean deleteAndPrepend(String key, byte[] value)
      throws ClientException {
    int size = getListSize();
    long expiry = getExpiry();
    if (size == 0 && expiry == 0) {
      return updateList(ListCommandID.ALIST_SWAP_AND_INSERT, key, value);
    } else if (size == 0 && expiry != 0) {
      return updateList(ListCommandID.ALIST_EXPIRED_SWAP_AND_INSERT, key, value);
    } else if (size != 0 && expiry == 0) {
      return updateList(ListCommandID.ALIST_SWAP_AND_SIZED_INSERT, key, value);
    } else { // size != 0 && expiry != 0
      return updateList(ListCommandID.ALIST_EXPIRED_SWAP_AND_SIZED_INSERT, key,
          value);
    }
  }

  public void deleteList(String key) throws ClientException {
    updateList(ListCommandID.ALIST_CLEAR, key, "".getBytes());
  }

  public boolean prepend(String key, byte[] value) throws ClientException {
    if (getListSize() == 0) {
      return updateList(ListCommandID.ALIST_INSERT, key, value);
    } else {
      return updateList(ListCommandID.ALIST_SIZED_INSERT, key, value);
    }
  }

  protected boolean updateList(int commandID, String key, byte[] value)
      throws ClientException {
    CommandContext context = new CommandContext();
    try {
      context.put(CommandContext.CONNECTION_POOL, client.getConnectionPool());
      context.put(CommandContext.ROUTING_TABLE, client.getRoutingTable());
      context.put(CommandContext.KEY, key);
      context.put(CommandContext.VALUE, value);
      if (commandID == ListCommandID.ALIST_INSERT) {
        // || commandID == ListCommandID.ALIST_SWAP_AND_INSERT
        // || commandID == ListCommandID.ALIST_SWAP_AND_PUSH
        context.put(UpdateCommand.INDEX, "0");
      } else if (commandID == ListCommandID.ALIST_SIZED_INSERT
          || commandID == ListCommandID.ALIST_SWAP_AND_SIZED_INSERT
          || commandID == ListCommandID.ALIST_EXPIRED_SWAP_AND_SIZED_INSERT
          || commandID == ListCommandID.ALIST_SIZED_PUSH
          || commandID == ListCommandID.ALIST_SWAP_AND_SIZED_PUSH
          || commandID == ListCommandID.ALIST_EXPIRED_SWAP_AND_SIZED_PUSH) {
        context.put(UpdateCommand.ARRAY_SIZE, (new Integer(getListSize()))
            .toString());
      }
      if (commandID == ListCommandID.ALIST_EXPIRED_SWAP_AND_INSERT
          || commandID == ListCommandID.ALIST_EXPIRED_SWAP_AND_SIZED_INSERT
          || commandID == ListCommandID.ALIST_EXPIRED_SWAP_AND_PUSH
          || commandID == ListCommandID.ALIST_EXPIRED_SWAP_AND_SIZED_PUSH) {
        context.put(UpdateCommand.EXPIRY, (new Long(getExpiry())).toString());
      }
      Command command = client.getCommandFactory().getCommand(commandID);
      context.put(CommandContext.COMMAND_ID, commandID);
      return client.exec(command, context);
    } catch (ClientException e) {
      LOG.error("update list failed: " + key, e);
      throw e;
    }
  }

  private static List<Entry> toEntryList(List<Object> input) {
    List<Entry> ret = new ArrayList<Entry>();
    for (Iterator<Object> iter = input.iterator(); iter.hasNext();) {
      byte[] v = (byte[]) iter.next();
      String t = (String) iter.next();
      Entry e = new Entry(v, t);
      ret.add(e);
    }
    return ret;
  }

  private static List<byte[]> toByteList(List<Entry> list) {
    List<byte[]> ret = new ArrayList<byte[]>();
    for (Iterator<Entry> iter = list.iterator(); iter.hasNext();) {
      Entry e = iter.next();
      ret.add(e.getValue());
    }
    return ret;
  }

  public List<byte[]> get(String key) throws ClientException {
    List<Entry> list = getEntries(key);
    return toByteList(list);
  }

  public List<Entry> getEntries(String key) throws ClientException {
    return toEntryList(get(ListCommandID.ALIST_GETS_WITH_TIME, key,
        JoinCommand.NULL));
  }

  public List<byte[]> get(String key, int begin, int len)
      throws ClientException {
    List<Entry> list = getEntries(key, begin, len);
    return toByteList(list);
  }

  public List<Entry> getEntries(String key, int begin, int len)
      throws ClientException {
    if (begin < 0 || len < 0) {
      throw new IllegalArgumentException();
    }
    StringBuilder sb = new StringBuilder();
    sb.append(begin).append(JoinCommand.RANGE).append(begin + len - 1);
    return toEntryList(get(ListCommandID.ALIST_GETS_WITH_TIME, key, sb
        .toString()));
  }

  @SuppressWarnings("unchecked")
  public List<Object> get(int commandID, String key, String value)
      throws ClientException {
    CommandContext context = new CommandContext();
    try {
      context.put(CommandContext.CONNECTION_POOL, client.getConnectionPool());
      context.put(CommandContext.ROUTING_TABLE, client.getRoutingTable());
      context.put(CommandContext.KEY, key);
      context.put(CommandContext.VALUE, value);
      Command command = client.getCommandFactory().getCommand(commandID);
      context.put(CommandContext.COMMAND_ID, commandID);
      boolean ret = client.exec(command, context);
      if (ret) {
        return (List<Object>) context.get(CommandContext.RESULT);
      } else {
        return new ArrayList<Object>();
      }
    } catch (ClientException e) {
      LOG.error("get failed: " + key, e);
      throw e;
    }
  }

  public int size(String key) throws ClientException {
    CommandContext context = new CommandContext();
    try {
      context.put(CommandContext.CONNECTION_POOL, client.getConnectionPool());
      context.put(CommandContext.ROUTING_TABLE, client.getRoutingTable());
      context.put(CommandContext.KEY, key);
      Command command = client.getCommandFactory().getCommand(
          ListCommandID.ALIST_LENGTH);
      context.put(CommandContext.COMMAND_ID, ListCommandID.ALIST_LENGTH);
      boolean ret = client.exec(command, context);
      if (ret) {
        Integer i = (Integer) context.get(CommandContext.RESULT);
        return i.intValue();
      } else {
        return 0;
      }
    } catch (ClientException e) {
      LOG.error("size failed: " + key, e);
      throw e;
    }
  }
}