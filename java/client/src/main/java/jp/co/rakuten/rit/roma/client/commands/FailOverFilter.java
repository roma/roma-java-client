package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import jp.co.rakuten.rit.roma.client.BadRoutingTableFormatException;
import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Config;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.ConnectionPool;
import jp.co.rakuten.rit.roma.client.Node;
import jp.co.rakuten.rit.roma.client.routing.RoutingTable;

/**
 * 
 */
public class FailOverFilter extends AbstractCommand {

    public static long sleepPeriod = Long.parseLong(Config.DEFAULT_RETRY_SLEEP_TIME);
    public static int retryThreshold = Integer.parseInt(Config.DEFAULT_RETRY_THRESHOLD);

    public FailOverFilter(Command next) {
    	super(next);
    }

    @Override
    public boolean execute(CommandContext context) throws ClientException {
        RoutingTable routingTable = (RoutingTable) context.get(CommandContext.ROUTING_TABLE);
        if (routingTable == null) {
            throw new ClientException(new BadRoutingTableFormatException(
                    "routing table is null."));
        }
        String key = (String) context.get(CommandContext.KEY);
        BigInteger hash = routingTable.getHash(key);
        if (hash == null) {
            throw new ClientException(new BadRoutingTableFormatException(
                    "hash is null."));
        }
        ConnectionPool connPool = (ConnectionPool)
	    context.get(CommandContext.CONNECTION_POOL);
        int retryCount = 0;
        List<String> errorList = new ArrayList<String>();
        String errorMessage = null;
        while (true) {
            Throwable t = null;
            try {
                Node node = routingTable.searchNode(key, hash);
                context.put(CommandContext.HASH, hash);
                context.put(CommandContext.NODE, node);
                return next.execute(context);
            } catch (ClientException e) {
                t = e.getCause();
                if (t != null) {
                    errorMessage = e.getMessage() + ":" + t.toString() + ":" + t.getMessage();
                } else {
                    errorMessage = e.getMessage() + "null";
                }
                errorList.add(errorMessage);
            }

            // re-try message-passing or handle an error
            if (t != null) {
                try {
                    Node node = (Node) context.get(CommandContext.NODE);
		    Connection conn = (Connection)
			context.get(CommandContext.CONNECTION);
                    if (t instanceof IOException || t instanceof TimeoutException) {
                        routingTable.incrFailCount(node);
                    } else if (t instanceof ClientException) {
                        if (t.getCause() instanceof IOException) {
                            routingTable.incrFailCount(node);
                        }
                    }
		    connPool.delete(node, conn);
                    Thread.sleep(sleepPeriod);
                } catch (InterruptedException e) { // ignore
                }
                if (retryCount < retryThreshold) {
                    retryCount++;
                } else {
                    ClientException e = new ClientException(new RetryOutException(errorList.toString()));
                    errorList.clear();
                    errorMessage = null;
                    throw e;
                }
            }
        }
    }

	@Override
	protected void create(CommandContext context) throws ClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	protected boolean parseResult(CommandContext context)
			throws ClientException {
		throw new UnsupportedOperationException();
	}

	@Override
	protected void sendAndReceive(CommandContext context) throws IOException,
			ClientException {
		throw new UnsupportedOperationException();
	}
}