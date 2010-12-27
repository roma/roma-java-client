package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Config;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.ConnectionPool;
import jp.co.rakuten.rit.roma.client.Node;

/**
 * 
 */
public class TimeoutFilter extends AbstractCommand {

    // The maximum time to wait (millis)
    public static long timeout = Long.parseLong(Config.DEFAULT_TIMEOUT_PERIOD);
    public static int numOfThreads = Integer
            .parseInt(Config.DEFAULT_NUM_OF_THREADS);
    private static ExecutorService executor;

    public TimeoutFilter(Command next) {
    	super(next);
    }

    public static void shutdown() {
        if (executor != null) {
            // executor.shutdown();
            executor.shutdownNow();
        }
        executor = null;
    }

    public TimeoutFilter() {
    }

    public static class CallableImpl implements Callable<Boolean> {

        private Command command;
        private CommandContext context;

        public CallableImpl(Command command, CommandContext context) {
            this.command = command;
            this.context = context;
        }

        public Boolean call() throws Exception {
            int commandID = (Integer) context.get(CommandContext.COMMAND_ID);
            Node node = null;
            ConnectionPool connPool = null;
            Connection conn = null;
            try {
                node = (Node) context.get(CommandContext.NODE);
                if (commandID != CommandID.ROUTING_DUMP &&
		    commandID != CommandID.ROUTING_MKLHASH) { // general commands
                    connPool = (ConnectionPool) context
                            .get(CommandContext.CONNECTION_POOL);
                    conn = connPool.get(node);
                    context.put(CommandContext.CONNECTION, conn);
                } else { // routingdump, routingmkh
                    Socket sock = new Socket(node.getHost(), node.getPort());
                    conn = new Connection(sock);
                    context.put(CommandContext.CONNECTION, conn);
                }
                return command.execute(context);
            } finally {
		conn = (Connection) context.get(CommandContext.CONNECTION);
		if (conn != null) {
		    try {
			if (commandID != CommandID.ROUTING_DUMP &&
			    commandID != CommandID.ROUTING_MKLHASH) { // general commands
			    connPool.put(node, conn);
			} else { // routingdump, routingmkh
			    conn.close();
			}
		    } catch (IOException e) { // ignore
		    }
		}
            }
        }
    }

    @Override
    public boolean execute(CommandContext context) throws ClientException {
        int commandID = (Integer) context.get(CommandContext.COMMAND_ID);
        if (executor == null) {
            if (numOfThreads > 0) {
                executor = Executors.newFixedThreadPool(numOfThreads);
            } else {
                executor = Executors.newCachedThreadPool();
            }
            // executor = Executors.newSingleThreadExecutor();
            // executor = Executors.newCachedThreadPool();
        }

        Callable<Boolean> task = new CallableImpl(next, context);
        Future<Boolean> future = executor.submit(task);
        Throwable t = null;
        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            t = e;
        } catch (CancellationException e) {
            t = e;
        } catch (ExecutionException e) {
            t = e.getCause();
        } catch (InterruptedException e) { // ignore
        }

        // error handling
        if (t != null) {
            if (t instanceof java.util.concurrent.TimeoutException) {
                ConnectionPool connPool = (ConnectionPool)
		    context.get(CommandContext.CONNECTION_POOL);
                Connection conn = (Connection)
		    context.get(CommandContext.CONNECTION);
                future.cancel(true);
                if (conn != null) {
		    try {
			if (commandID != CommandID.ROUTING_DUMP &&
			    commandID != CommandID.ROUTING_MKLHASH) { // general commands
			    Node node = (Node) context.get(CommandContext.NODE);
			    connPool.delete(node, conn);
			} else { // routingdump, routingmkh
                            conn.close();
			}
                    } catch (IOException e) { // ignore
		    }
                }
                throw new ClientException(new TimeoutException(t));
            } else { // otherwise
                if (t instanceof ClientException) {
                    throw (ClientException) t;
                } else {
                    throw new ClientException(t);
                }
            }
        }
        return false;
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
