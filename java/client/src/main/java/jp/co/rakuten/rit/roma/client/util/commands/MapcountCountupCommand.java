package jp.co.rakuten.rit.roma.client.util.commands;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.commands.AbstractCommand;
import jp.co.rakuten.rit.roma.client.commands.CommandContext;
import net.arnx.jsonic.JSON;

public class MapcountCountupCommand extends AbstractCommand {
  @SuppressWarnings("unchecked")
  @Override
  public boolean execute(CommandContext context) throws ClientException {
    try {
      // mapcount_countup <key> [<exptime> <bytes>]\r\n
      // [<sub_keys>]\r\n
      StringBuilder sb1 = new StringBuilder();
      StringBuilder sb2 = new StringBuilder();
      HashMap<String, Integer> keys = (HashMap<String, Integer>) context.get(CommandContext.KEYS);

      Object obj = context.get(CommandContext.EXPIRY);
      String expiry;
      if (obj instanceof Date) {
        // the type of object is deprecated
        expiry = "" + ((long) (((Date) obj).getTime() / 1000));
      } else {
        expiry = (String) obj;
      }

      sb1.append(MapcountCommandID.STR_MAPCOUNT_COUNTUP)
         .append(STR_WHITE_SPACE)
         .append(context.get(CommandContext.KEY));

      Set<String> subKeys = keys.keySet();
      String subKey = null;
      for (Iterator<String> iter = subKeys.iterator(); iter.hasNext();) {
        subKey = iter.next();
        sb2.append(subKey)
           .append(STR_COLON)
           .append(Integer.toString(keys.get(subKey)))
           .append(STR_COMMA);
      }
      sb2.deleteCharAt(sb2.length() - 1);

      sb1.append(STR_WHITE_SPACE)
         .append(expiry)
         .append(STR_WHITE_SPACE)
         .append(String.valueOf(sb2.length()));       

      sb1.append(STR_CRLF);
      sb2.append(STR_CRLF);

      Connection conn = (Connection) context.get(CommandContext.CONNECTION);
      conn.out.write(sb1.toString().getBytes());
      conn.out.write(sb2.toString().getBytes());
      conn.out.flush();

      // VALUE <key> <flag> <bytes>\r\n<data block>\r\n or END\r\n
      Object values = null;
      String s;
      s = conn.in.readLine();
      if (s.startsWith("VALUE")) {
        values = new Object();
      } else if (s.startsWith("END")) {
        return false;
      } else if (s.startsWith("SERVER_ERROR")
          || s.startsWith("CLIENT_ERROR")
          || s.startsWith("ERROR")) {
        throw new ClientException(s);
      } else {
        throw new ClientException("Not supported yet.");
      }

      // <data block>
      values = JSON.decode(conn.in.readLine());

      context.put(CommandContext.RESULT, values);
      return true;
    } catch (IOException e) {
      throw new ClientException(e);
    }
  }

  @Override
  protected void sendAndReceive(CommandContext context) throws IOException,
      ClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void create(CommandContext context) throws ClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected boolean parseResult(CommandContext context) throws ClientException {
    throw new UnsupportedOperationException();
  }
}
