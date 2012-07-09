package jp.co.rakuten.rit.roma.client.commands;

import jp.co.rakuten.rit.roma.client.ClientException;

public class ExpireCommand extends StoreCommand {
  @Override
  public void create(CommandContext context) throws ClientException {
    StringBuilder sb = (StringBuilder) context.get(CommandContext.STRING_DATA);
    sb.append(getCommand())
      .append(STR_WHITE_SPACE)
      .append(context.get(CommandContext.KEY))
      .append(STR_ESC)
      .append(context.get(CommandContext.HASH_NAME))
      .append(STR_WHITE_SPACE)
      .append(context.get(CommandContext.EXPIRY))
      .append(STR_CRLF);
    context.put(CommandContext.STRING_DATA, sb);
  }

  protected String getCommand() throws ClientException {
    return STR_EXPIRE;
  }
}