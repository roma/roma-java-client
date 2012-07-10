package jp.co.rakuten.rit.roma.client.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.CommandFactory;
import jp.co.rakuten.rit.roma.client.RomaClient;
import jp.co.rakuten.rit.roma.client.commands.Command;
import jp.co.rakuten.rit.roma.client.commands.CommandContext;
import jp.co.rakuten.rit.roma.client.commands.FailOverFilter;
import jp.co.rakuten.rit.roma.client.commands.TimeoutFilter;
import jp.co.rakuten.rit.roma.client.util.commands.MapcountCommandID;
import jp.co.rakuten.rit.roma.client.util.commands.MapcountCountupCommand;
import jp.co.rakuten.rit.roma.client.util.commands.MapcountGetCommand;
import jp.co.rakuten.rit.roma.client.util.commands.MapcountUpdateCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Sample code might look like:
 * 
 * <blockquote>
 * 
 * <pre>
 * </pre>
 * 
 * </blockquote>
 */

public class MapcountWrapper {

  private static Logger LOG = LoggerFactory.getLogger(MapcountWrapper.class);

  protected RomaClient client;

  public MapcountWrapper(RomaClient client) throws ClientException {
    this.client = client;
    CommandFactory commandFact = client.getCommandFactory();
    commandFact.createCommand(MapcountCommandID.MAPCOUNT_GET,
        new FailOverFilter(new TimeoutFilter(new MapcountGetCommand())));
    commandFact.createCommand(MapcountCommandID.MAPCOUNT_COUNTUP,
        new FailOverFilter(new TimeoutFilter(new MapcountCountupCommand())));
    commandFact.createCommand(MapcountCommandID.MAPCOUNT_UPDATE,
        new FailOverFilter(new TimeoutFilter(new MapcountUpdateCommand())));
  }

  public Object get(String key, List<String> keys) throws ClientException {
    return getExec(key, keys);
  }

  public Object get(String key) throws ClientException {
    return getExec(key, new ArrayList<String>());
  }

  private Object getExec(String key, List<String> keys) throws ClientException {
    CommandContext context = new CommandContext();
    try {
      context.put(CommandContext.CONNECTION_POOL, client.getConnectionPool());
      context.put(CommandContext.ROUTING_TABLE, client.getRoutingTable());
      context.put(CommandContext.KEY, key);
      context.put(CommandContext.KEYS, keys);
      context.put(CommandContext.COMMAND_ID, MapcountCommandID.MAPCOUNT_GET);
      Command command = client.getCommandFactory().getCommand(
          MapcountCommandID.MAPCOUNT_GET);
      boolean ret = client.exec(command, context);
      if (ret) {
        return context.get(CommandContext.RESULT);
      } else {
        return null;
      }
    } catch (ClientException e) {
      LOG.error("get failed: " + key, e);
      throw e;
    }
  }

  public Object countup(String key, Map<String, Integer> keys) throws ClientException {
    return countupExec(key, keys, 0);
  }

  public Object countup(String key, Map<String, Integer> keys, long expiry) throws ClientException {
    return countupExec(key, keys, expiry);
  }

  public Object countupExec(String key, Map<String, Integer> keys, long expiry)
      throws ClientException {
    CommandContext context = new CommandContext();
    try {
      context.put(CommandContext.CONNECTION_POOL, client.getConnectionPool());
      context.put(CommandContext.ROUTING_TABLE, client.getRoutingTable());
      context.put(CommandContext.KEY, key);
      context.put(CommandContext.KEYS, keys);
      context.put(CommandContext.EXPIRY, "" + expiry);
      context
          .put(CommandContext.COMMAND_ID, MapcountCommandID.MAPCOUNT_COUNTUP);
      Command command = client.getCommandFactory().getCommand(
          MapcountCommandID.MAPCOUNT_COUNTUP);
      boolean ret = client.exec(command, context);
      if (ret) {
        return context.get(CommandContext.RESULT);
      } else {
        return null;
      }
    } catch (ClientException e) {
      LOG.error("countup failed: " + key, e);
      throw e;
    }
  }

  public Object update(String key) throws ClientException {
    return updateExec(key, new ArrayList<String>(), 0);
  }

  public Object update(String key, List<String> keys) throws ClientException {
    return updateExec(key, keys, 0);
  }

  public Object update(String key, long expiry) throws ClientException {
    return updateExec(key, new ArrayList<String>(), expiry);
  }

  public Object update(String key, List<String> keys, long expiry) throws ClientException {
    return updateExec(key, keys, expiry);
  }

  public Object updateExec(String key, List<String> keys, long expiry)
      throws ClientException {
    CommandContext context = new CommandContext();
    try {
      context.put(CommandContext.CONNECTION_POOL, client.getConnectionPool());
      context.put(CommandContext.ROUTING_TABLE, client.getRoutingTable());
      context.put(CommandContext.KEY, key);
      context.put(CommandContext.KEYS, keys);
      context.put(CommandContext.EXPIRY, "" + expiry);
      context.put(CommandContext.COMMAND_ID, MapcountCommandID.MAPCOUNT_UPDATE);
      Command command = client.getCommandFactory().getCommand(
          MapcountCommandID.MAPCOUNT_UPDATE);
      boolean ret = client.exec(command, context);
      if (ret) {
        return context.get(CommandContext.RESULT);
      } else {
        return null;
      }
    } catch (ClientException e) {
      LOG.error("update failed: " + key, e);
      throw e;
    }
  }
}
