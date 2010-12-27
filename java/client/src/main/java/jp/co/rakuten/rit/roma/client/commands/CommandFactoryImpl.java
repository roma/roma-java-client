package jp.co.rakuten.rit.roma.client.commands;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.CommandFactory;

/**
 * 
 */
public class CommandFactoryImpl implements CommandFactory {

    // protected HashMap<Integer, Command> commands = new HashMap<Integer, Command>();
    protected Map<Integer, Command> commands = new ConcurrentHashMap<Integer, Command>();

    public CommandFactoryImpl() throws ClientException {
        init();
    }

    protected void init() throws ClientException {
    	createCommand(CommandID.GET, new FailOverFilter(new TimeoutFilter(new GetCommand())));
    	createCommand(CommandID.GETS, new FailOverFilter(new TimeoutFilter(new GetsCommand())));
    	createCommand(CommandID.GETS_OPT, new FailOverFilter(new TimeoutFilter(new GetsOptCommand())));
    	createCommand(CommandID.GETS_WITH_CASID, new FailOverFilter(new TimeoutFilter(new GetsWithCasIDCommand())));
    	createCommand(CommandID.GETS_WITH_CASID_OPT, new FailOverFilter(new TimeoutFilter(new GetsWithCasIDOptCommand())));
    	createCommand(CommandID.SET, new FailOverFilter(new TimeoutFilter(new SetCommand())));
    	createCommand(CommandID.ADD, new FailOverFilter(new TimeoutFilter(new AddCommand())));
    	createCommand(CommandID.APPEND, new FailOverFilter(new TimeoutFilter(new AppendCommand())));
    	createCommand(CommandID.PREPEND, new FailOverFilter(new TimeoutFilter(new PrependCommand())));
    	createCommand(CommandID.DELETE, new FailOverFilter(new TimeoutFilter(new DeleteCommand())));
    	createCommand(CommandID.INCREMENT, new FailOverFilter(new TimeoutFilter(new IncrCommand())));
    	createCommand(CommandID.DECREMENT, new FailOverFilter(new TimeoutFilter(new DecrCommand())));
    	createCommand(CommandID.CAS, new FailOverFilter(new TimeoutFilter(new CasCommand())));
    	createCommand(CommandID.EXPIRE, new FailOverFilter(new TimeoutFilter(new ExpireCommand())));
    	createCommand(CommandID.ROUTING_DUMP, new TimeoutFilter(new RoutingdumpCommand()));
    	createCommand(CommandID.ROUTING_MKLHASH, new TimeoutFilter(new RoutingmhtCommand()));
    }

    public Command getCommand(final int commandID) {
        Command command = commands.get(new Integer(commandID));
        if (command == null) {
            throw new NullPointerException("command is not defined: #"
                    + commandID);
        }
        return command;
    }

    public void createCommand(final int commandID, Command command) {
        commands.put(new Integer(commandID), command);
    }

}
