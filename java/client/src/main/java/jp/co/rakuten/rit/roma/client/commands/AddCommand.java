package jp.co.rakuten.rit.roma.client.commands;

public class AddCommand extends StoreCommand {
    @Override
    public String getCommand() throws BadCommandException {
        return STR_ADD;
    }
}
