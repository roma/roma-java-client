package jp.co.rakuten.rit.roma.client;

import jp.co.rakuten.rit.roma.client.commands.Command;

public interface CommandFactory {

    public Command getCommand(int commandID);

    public void createCommand(int commandID, Command command);
}
