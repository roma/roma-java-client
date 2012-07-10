package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Connection;

public class DeleteCommand extends AbstractCommand implements CommandID {
  @Override
  protected void create(CommandContext context) throws ClientException {
    // delete <key> [<time>] [noreply]\r\n
    StringBuilder sb = (StringBuilder) context.get(CommandContext.STRING_DATA);
    sb.append(STR_DELETE)
      .append(STR_WHITE_SPACE)
      .append(context.get(CommandContext.KEY))
      .append(STR_ESC)
      .append(context.get(CommandContext.HASH_NAME))
      .append(STR_CRLF);
    context.put(CommandContext.STRING_DATA, sb);
  }

  @Override
  protected void sendAndReceive(CommandContext context) throws IOException,
      ClientException {
    StringBuilder sb = (StringBuilder) context.get(CommandContext.STRING_DATA);
    Connection conn = (Connection) context.get(CommandContext.CONNECTION);
    conn.out.write(sb.toString().getBytes());
    conn.out.flush();

    sb = new StringBuilder();
    sb.append(conn.in.readLine());
    context.put(CommandContext.STRING_DATA, sb);
  }

  @Override
  protected boolean parseResult(CommandContext context) throws ClientException {
    StringBuilder sb = (StringBuilder) context.get(CommandContext.STRING_DATA);
    String s = sb.toString();
    if (s.startsWith("DELETED")) {
      return true;
    } else if (s.startsWith("NOT_FOUND")) {
      return false;
    } else if (s.startsWith("SERVER_ERROR") || s.startsWith("CLIENT_ERROR")
        || s.startsWith("ERROR")) {
      throw new ClientException(s);
    } else {
      throw new ClientException("not support yet");
    }
  }
}