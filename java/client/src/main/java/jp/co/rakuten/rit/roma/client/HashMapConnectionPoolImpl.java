package jp.co.rakuten.rit.roma.client;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HashMapConnectionPoolImpl implements ConnectionPool {
  protected int size;

  HashMap<Node, List<Connection>> pool = new HashMap<Node, List<Connection>>();

  public HashMapConnectionPoolImpl(final int size) {
    this.size = size;
  }

  public synchronized Connection get(Node node) throws IOException {
    List<Connection> conns = pool.get(node);
    if (conns == null) {
      conns = new ArrayList<Connection>();
      pool.put(node, conns);
    }
    if (conns.size() == 0) {
      conns.add(new Connection(new Socket(node.host, node.port)));
    }
    return conns.remove(0);
  }

  public synchronized void put(Node node, Connection conn) throws IOException {
    if (conn == null) {
      return;
    }
    List<Connection> conns = pool.get(node);
    if (conns == null) {
      conn.close();
      return;
    }
    if (size > 0 && conns.size() >= size) {
      conn.close();
      return;
    }
    conns.add(conn);
  }

  public synchronized void delete(Node node, Connection conn) {
    if (conn != null) {
      try {
        conn.close();
      } catch (IOException e) {
        // ConnectionPool#delete cannot report failures.
      }
    }
  }

  public synchronized void deleteAll(Node node) {
    List<Connection> conns = pool.remove(node);
    if (conns == null) {
      return;
    }
    for (Connection conn : conns) {
      try {
        conn.close();
      } catch (IOException e) {
        // ConnectionPool#deleteAll cannot report failures.
      }
    }
  }

  public synchronized void closeAll() {
    for (List<Connection> conns : pool.values()) {
      for (Connection conn : conns) {
        try {
          conn.close();
        } catch (IOException e) {
          // ConnectionPool#closeAll cannot report failures.
        }
      }
    }
    pool.clear();
  }
}
