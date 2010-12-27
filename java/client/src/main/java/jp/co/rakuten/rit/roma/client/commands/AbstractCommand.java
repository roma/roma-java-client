package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;

import jp.co.rakuten.rit.roma.client.ClientException;

/**
 * 
 */
public abstract class AbstractCommand implements Command, CommandID {

	protected Command next;

	protected AbstractCommand() {
		this(null);
	}

	protected AbstractCommand(Command next) {
		this.next = next;
	}

    public boolean execute(CommandContext context) throws ClientException {
    	try {
    		StringBuilder sb = new StringBuilder();
    		context.put(CommandContext.STRING_DATA, sb);
    		create(context);
    		sendAndReceive(context);
    		return parseResult(context);
    	} catch (IOException e) {
    		throw new ClientException(e);
    	}
    }

    protected abstract void create(CommandContext context) throws ClientException;

    protected abstract void sendAndReceive(CommandContext context)
            throws IOException, ClientException;

    protected abstract boolean parseResult(CommandContext context)
            throws ClientException;
}
