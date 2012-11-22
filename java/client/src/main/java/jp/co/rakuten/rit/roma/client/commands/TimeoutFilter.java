package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;
import java.net.Socket;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Config;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.ConnectionPool;
import jp.co.rakuten.rit.roma.client.Node;

public class TimeoutFilter extends AbstractCommand {
  // The maximum time to wait (millis)
  public static long timeout = Long.parseLong(Config.DEFAULT_TIMEOUT_PERIOD);
  public static int numOfThreads = Integer
      .parseInt(Config.DEFAULT_NUM_OF_THREADS);

  public TimeoutFilter(Command next) {
    super(next);
  }

  // public static void shutdown() { }

  public TimeoutFilter() {
  }

  @Override
  public boolean execute(CommandContext context) throws ClientException {
    int commandID = (Integer) context.get(CommandContext.COMMAND_ID);
    Node node = null;
    ConnectionPool connPool = null;
    Connection conn = null;
    Throwable t = null;
    boolean ret = false;
    boolean usepool = commandID != CommandID.ROUTING_DUMP
        && commandID != CommandID.ROUTING_MKLHASH;
    try {
      node = (Node) context.get(CommandContext.NODE);
      if (usepool) { // general commands
        connPool = (ConnectionPool) context.get(CommandContext.CONNECTION_POOL);
        conn = connPool.get(node);
        conn.setTimeout((int) timeout);
        context.put(CommandContext.CONNECTION, conn);
      } else { // routingdump, routingmkh
        Socket sock = new Socket(node.getHost(), node.getPort());
        conn = new Connection(sock);
        conn.setTimeout((int) timeout);
        context.put(CommandContext.CONNECTION, conn);
      }
      ret = next.execute(context);

      if (conn != null) {
        if (usepool) { // general commands
          conn.setTimeout(0);
          connPool.put(node, conn);
        } else { // routingdump, routingmkh
          try {
            if (conn != null)
              conn.close();
          } catch (IOException e1) {
          }
        }
      }
      return ret;
    } catch (java.net.SocketTimeoutException e) {
      t = e;
    } catch (IOException e) {
      t = e;
    }

    if (usepool) { // general commands
      connPool.delete(node, conn);
    } else { // routingdump, routingmkh
      try {
        if (conn != null)
          conn.close();
      } catch (IOException e1) {
      }
    }
    throw new ClientException(new TimeoutException(t));
  }

  @Override
  protected void create(CommandContext context) throws ClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean parseResult(CommandContext context) throws ClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void sendAndReceive(CommandContext context) throws IOException,
      ClientException {
    throw new UnsupportedOperationException();
  }
}