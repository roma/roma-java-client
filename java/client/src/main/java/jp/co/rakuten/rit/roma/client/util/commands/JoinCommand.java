package jp.co.rakuten.rit.roma.client.util.commands;

import java.io.IOException;
import java.util.StringTokenizer;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.commands.AbstractCommand;
import jp.co.rakuten.rit.roma.client.commands.CommandContext;

public class JoinCommand extends AbstractCommand {
  public static final String SEP = "_$$_";
  public static final String NULL = "";
  public static final String RANGE = "..";

  @Override
  public boolean execute(CommandContext context) throws ClientException {
    try {
      // alist_join key <bytes>\r\n
      // value\r\n
      StringBuilder sb = new StringBuilder();
      sb.append(ListCommandID.STR_ALIST_JOIN)
        .append(ListCommandID.STR_WHITE_SPACE)
        .append(context.get(CommandContext.KEY))
        .append(ListCommandID.STR_WHITE_SPACE)
        .append(SEP.getBytes().length);
      String range = (String) context.get(CommandContext.VALUE);
      if (!range.equals(NULL)) {
        sb.append(ListCommandID.STR_WHITE_SPACE).append((String) range);
      }
      sb.append(ListCommandID.STR_CRLF).append(SEP).append(
          ListCommandID.STR_CRLF);
      Connection conn = (Connection) context.get(CommandContext.CONNECTION);
      conn.out.write(sb.toString().getBytes());
      conn.out.flush();

      // VALUE <key> 0 <len>\r\n
      // <size>\r\n
      // VALUE <key> 0 <byte_len>\r\n
      // <values>\r\n
      // END\r\n
      // or SERVER_ERROR
      String s;
      s = conn.in.readLine();
      if (s.startsWith("VALUE")) {
        ;
      } else if (s.startsWith("END")) {
        // return null;
        return false;
      } else if (s.startsWith("SERVER_ERROR") || s.startsWith("CLIENT_ERROR")
          || s.startsWith("ERROR")) {
        throw new ClientException(s);
      } else {
        throw new ClientException("Not supported yet.");
      }

      // int size = Integer.parseInt(conn.in.readLine()); // <size>\r\n
      conn.in.readLine(); // <size>\r\n
      s = conn.in.readLine(); // VALUE <key> 0 <byte len>\r\n
      StringTokenizer t = new StringTokenizer(s);
      t.nextToken(); // "VALUE"
      t.nextToken(); // key
      t.nextToken(); // 0
      int valueLen = Integer.parseInt(t.nextToken()); // byte len

      // value
      byte[] value = new byte[valueLen];
      int offset = 0;
      int size = 0;
      while (offset < valueLen) {
        size = conn.in.read(value, offset, valueLen - offset);
        offset = offset + size;
      }
      context.put(CommandContext.RESULT, value);

      conn.in.read(2); // "\r\n"
      conn.in.readLine(); // "END\r\n"
      // return value;
      return true;
    } catch (IOException e) {
      throw new ClientException(e);
    }
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