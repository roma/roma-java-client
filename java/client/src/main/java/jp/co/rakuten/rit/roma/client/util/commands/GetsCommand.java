package jp.co.rakuten.rit.roma.client.util.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.commands.AbstractCommand;
import jp.co.rakuten.rit.roma.client.commands.CommandContext;

public class GetsCommand extends AbstractCommand {
  @Override
  public boolean execute(CommandContext context) throws ClientException {
    try {
      // alist_gets <key> [index|range] [forward]\r\n #
      StringBuilder sb = new StringBuilder();
      sb.append(ListCommandID.STR_ALIST_GETS)
        .append(ListCommandID.STR_WHITE_SPACE)
        .append(context.get(CommandContext.KEY));
      String range = (String) context.get(CommandContext.VALUE);
      if (!range.equals(JoinCommand.NULL)) {
        sb.append(ListCommandID.STR_WHITE_SPACE).append(range);
      }
      sb.append(ListCommandID.STR_CRLF);
      Connection conn = (Connection) context.get(CommandContext.CONNECTION);
      conn.out.write(sb.toString().getBytes());
      conn.out.flush();

      // (
      // [
      // VALUE <key> 0 <length of length string>\r\n
      // <length string>\r\n
      // (VALUE <key> 0 <value length>\r\n
      // <value>\r\n)*
      // ]
      // END\r\n
      // | SERVER_ERROR <error message>\r\n
      // )
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
      conn.in.readLine(); // "length\r\n"

      s = null;
      List<Object> ret = new ArrayList<Object>();
      while (true) {
        s = conn.in.readLine();
        if (s.startsWith("END")) { // "END\r\n"
          break;
        }

        // "VALUE <key> 0 <value len>\r\n"
        StringTokenizer t = new StringTokenizer(s);
        t.nextToken(); // "VALUE"
        t.nextToken(); // key
        t.nextToken(); // 0
        int valueLen = Integer.parseInt(t.nextToken()); // "<value len>"
        byte[] value = new byte[valueLen];
        int offset = 0;
        int size = 0;
        while (offset < valueLen) { // value
          size = conn.in.read(value, offset, valueLen - offset);
          offset = offset + size;
        }
        ret.add(value);
        conn.in.read(2); // "\r\n"
      }
      context.put(CommandContext.RESULT, ret);
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