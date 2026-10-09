package jp.co.rakuten.rit.roma.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import jp.co.rakuten.rit.roma.client.commands.CommandContext;
import jp.co.rakuten.rit.roma.client.commands.GetCommand;
import jp.co.rakuten.rit.roma.client.commands.GetsOptCommand;
import jp.co.rakuten.rit.roma.client.commands.SetCommand;
import junit.framework.TestCase;

/** Unit tests for wire-level input handling; no ROMA server is required. */
public class ConnectionTest extends TestCase {
  private static class StubSocket extends Socket {
    private final InputStream input;
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();

    StubSocket(byte[] bytes) {
      input = new ByteArrayInputStream(bytes);
    }

    @Override
    public InputStream getInputStream() {
      return input;
    }

    @Override
    public OutputStream getOutputStream() {
      return output;
    }

    @Override
    public void setSoTimeout(int timeout) {
      // The in-memory socket has no blocking read to time out.
    }

    String written() throws Exception {
      return output.toString("US-ASCII");
    }
  }

  private static class CountingConnectionPool implements ConnectionPool {
    private final Connection connection;
    int puts;
    int deletes;

    CountingConnectionPool(Connection connection) {
      this.connection = connection;
    }

    public Connection get(Node node) {
      return connection;
    }

    public void put(Node node, Connection conn) {
      puts++;
    }

    public void delete(Node node, Connection conn) {
      deletes++;
    }

    public void deleteAll(Node node) {
    }

    public void closeAll() {
    }
  }

  public void testReadLineReadsCrLfFrame() throws Exception {
    Connection connection = new Connection(new StubSocket("STORED\r\n".getBytes("US-ASCII")));
    assertEquals("STORED", connection.in.readLine());
  }

  public void testReadLineRejectsTruncatedFrame() throws Exception {
    Connection connection = new Connection(new StubSocket("VALUE key 0 3\r".getBytes("US-ASCII")));
    try {
      connection.in.readLine();
      fail("expected IOException");
    } catch (IOException expected) {
      // expected
    }
  }

  public void testReadFullyRejectsTruncatedValue() throws Exception {
    Connection connection = new Connection(new StubSocket(new byte[] { 1, 2 }));
    try {
      connection.in.readFully(new byte[3], 0, 3);
      fail("expected IOException");
    } catch (IOException expected) {
      // expected
    }
  }

  public void testGetRequestPreservesRomaEscSuffix() throws Exception {
    StubSocket socket = new StubSocket(
        "VALUE key 0 3\r\none\r\nEND\r\n".getBytes("US-ASCII"));
    CommandContext context = new CommandContext();
    context.put(CommandContext.CONNECTION, new Connection(socket));
    context.put(CommandContext.KEY, "key");
    context.put(CommandContext.HASH_NAME, "roma");

    assertTrue(new GetCommand().execute(context));
    assertEquals("get key\u001broma\r\n", socket.written());
    assertEquals("one", new String((byte[]) context.get(CommandContext.RESULT),
        "US-ASCII"));
  }

  public void testSetRequestPreservesRomaWireFormat() throws Exception {
    StubSocket socket = new StubSocket("STORED\r\n".getBytes("US-ASCII"));
    CommandContext context = new CommandContext();
    context.put(CommandContext.CONNECTION, new Connection(socket));
    context.put(CommandContext.KEY, "key");
    context.put(CommandContext.HASH_NAME, "roma");
    context.put(CommandContext.HASH, "123");
    context.put(CommandContext.EXPIRY, "0");
    context.put(CommandContext.VALUE, "one".getBytes("US-ASCII"));

    assertTrue(new SetCommand().execute(context));
    assertEquals("set key\u001broma 123 0 3\r\none\r\n", socket.written());
  }

  public void testOptimizedGetsReturnsHealthyConnectionToPool() throws Exception {
    StubSocket socket = new StubSocket("END\r\n".getBytes("US-ASCII"));
    CountingConnectionPool pool = new CountingConnectionPool(new Connection(socket));
    CommandContext context = new CommandContext();
    context.put(CommandContext.CONNECTION_POOL, pool);
    context.put(CommandContext.HASH_NAME, "roma");

    java.util.List<String> keys = new java.util.ArrayList<String>();
    keys.add("key");
    GetsOptCommand.CallableImpl call = new GetsOptCommand.CallableImpl(context,
        Node.create("localhost_11211"), keys);

    assertTrue(call.call().isEmpty());
    assertEquals(1, pool.puts);
    assertEquals(0, pool.deletes);
    assertEquals("gets key\u001broma\r\n", socket.written());
  }

  public void testOptimizedGetsInvalidatesFailedConnection() throws Exception {
    StubSocket socket = new StubSocket("SERVER_ERROR failed\r\n".getBytes(
        "US-ASCII"));
    CountingConnectionPool pool = new CountingConnectionPool(new Connection(socket));
    CommandContext context = new CommandContext();
    context.put(CommandContext.CONNECTION_POOL, pool);
    context.put(CommandContext.HASH_NAME, "roma");

    java.util.List<String> keys = new java.util.ArrayList<String>();
    keys.add("key");
    GetsOptCommand.CallableImpl call = new GetsOptCommand.CallableImpl(context,
        Node.create("localhost_11211"), keys);

    try {
      call.call();
      fail("expected ClientException");
    } catch (ClientException expected) {
      // expected
    }
    assertEquals(0, pool.puts);
    assertEquals(1, pool.deletes);
  }
}
