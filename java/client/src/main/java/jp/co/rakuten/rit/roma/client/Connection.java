package jp.co.rakuten.rit.roma.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

/**
 * A connection to a ROMA process.
 * 
 */
public class Connection {
  public Socket sock;

  public ExtInputStream in;

  public OutputStream out;

  /**
   * Construct a connection to a ROMA process.
   * 
   * @param sock
   *          - a socket of the ROMA process to connect to
   * @throws IOException
   *           - if creating this connection fails
   */
  public Connection(final Socket sock) throws IOException {
    this.sock = sock;
    this.in = new ExtInputStream(sock.getInputStream());
    this.out = new BufferedOutputStream(sock.getOutputStream());
  }

  public void setTimeout(int timeout) throws SocketException {
	  this.sock.setSoTimeout(timeout);
  }
  
  /**
   * Close this connection.
   * 
   * @throws IOException
   *           - if closing this connection fails
   */
  public void close() throws IOException {
    IOException failure = null;
    try {
      if (in != null) {
        in.close();
      }
    } catch (IOException e) {
      failure = e;
    }
    try {
      if (out != null) {
        out.close();
      }
    } catch (IOException e) {
      if (failure == null) {
        failure = e;
      }
    }
    try {
      if (sock != null) {
        sock.close();
      }
    } catch (IOException e) {
      if (failure == null) {
        failure = e;
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  public static long TIME = 0;

  public class ExtInputStream extends BufferedInputStream {

    private byte[] one_byte = new byte[1];

    ExtInputStream(InputStream in) {
      super(in);
    }

    public void read(int byteLen) throws IOException {
      for (int i = 0; i < byteLen; ++i) {
        if (super.read(one_byte) < 0) {
          throw new IOException("unexpected end of stream");
        }
      }
    }

    /**
     * Reads exactly {@code byteLen} bytes or throws when the peer closes the
     * connection. InputStream.read is allowed to return fewer bytes than were
     * requested, which is common on a network socket.
     */
    public void readFully(byte[] bytes, int offset, int byteLen)
        throws IOException {
      int end = offset + byteLen;
      while (offset < end) {
        int count = super.read(bytes, offset, end - offset);
        if (count < 0) {
          throw new IOException("unexpected end of stream");
        }
        if (count == 0) {
          continue;
        }
        offset += count;
      }
    }

    public String readLine() throws IOException {
      StringBuilder ret = new StringBuilder();

      long t = System.currentTimeMillis();

      while (true) {
        if (super.read(one_byte) < 0) {
          throw new IOException("unexpected end of stream");
        }
        if (one_byte[0] == 0x0d) { // \r
          if (super.read(one_byte) < 0 || one_byte[0] != 0x0a) {
            throw new IOException("invalid line terminator");
          }
          break;
        } else {
          ret.append((char) one_byte[0]);
        }
      }

      synchronized (Connection.class) {
        TIME = TIME + (System.currentTimeMillis() - t);
      }
      return ret.toString();
    }
  }
}
