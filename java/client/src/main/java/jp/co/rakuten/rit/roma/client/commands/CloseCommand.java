package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;

import jp.co.rakuten.rit.roma.client.ClientException;

public class CloseCommand extends AbstractCommand {
  @Override
  protected void create(CommandContext context) throws ClientException {
  }

  @Override
  protected boolean parseResult(CommandContext context) throws ClientException {
    return false;
  }

  @Override
  protected void sendAndReceive(CommandContext context) throws IOException,
      ClientException {
  }
}