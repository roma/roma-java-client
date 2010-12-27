package jp.co.rakuten.rit.roma.client.commands;

import jp.co.rakuten.rit.roma.client.ClientException;

public class AddCommand extends StoreCommand {
    @Override
    public String getCommand() throws ClientException {
        return STR_ADD;
    }
}
