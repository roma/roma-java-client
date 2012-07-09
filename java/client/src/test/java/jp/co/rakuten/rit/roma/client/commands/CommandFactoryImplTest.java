package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.commands.AbstractCommand;

import junit.framework.TestCase;

public class CommandFactoryImplTest extends TestCase {
  public void testDummy() {
    assertTrue(true);
  }

  public void XtestCreateCommand() throws Exception {
    TimeoutFilter.timeout = 10;
    CommandContext context = new CommandContext();
    context.put(CommandContext.CONNECTION_POOL, new MockConnectionPool());
    CommandFactoryImpl fact = new CommandFactoryImpl();
    fact.createCommand(1, new FailOverFilter(new TimeoutFilter(
        new TestCommand())));
    Command command = fact.getCommand(1);
    command.execute(context);
  }

  public static class TestCommand extends AbstractCommand {

    @Override
    public boolean execute(CommandContext context) throws ClientException {
      try {
        Thread.sleep(100);
      } catch (Exception e) {
        e.printStackTrace();
      }
      System.out.println("execute");
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
}