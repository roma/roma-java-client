package jp.co.rakuten.rit.roma.client.commands;

import jp.co.rakuten.rit.roma.client.ClientException;

public interface Command {
  boolean execute(CommandContext context) throws ClientException;
}