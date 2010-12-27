package jp.co.rakuten.rit.roma.client.commands;

public interface CommandGenerator {

    public Command getCommand(int commandID);

    public void createCommand(int commandID, Command command);
}
