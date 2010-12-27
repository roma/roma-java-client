package jp.co.rakuten.rit.roma.client.util.commands;

import java.io.IOException;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.commands.AbstractCommand;
import jp.co.rakuten.rit.roma.client.commands.CommandContext;

/**
 * 
 */
public class DeleteAtCommand extends AbstractCommand {

    @Override
    public boolean execute(CommandContext context) throws ClientException {
        try {
            // alist_delete_at <key> <index>\r\n
            StringBuilder sb = new StringBuilder();
            sb.append(ListCommandID.STR_ALIST_DELETE_AT).append(
                    ListCommandID.STR_WHITE_SPACE).append(
                    context.get(CommandContext.KEY)).append(
                    ListCommandID.STR_WHITE_SPACE).append(
                    new String((byte[]) context.get(CommandContext.VALUE)))
                    .append(ListCommandID.STR_CRLF);

            Connection conn = (Connection) context
                    .get(CommandContext.CONNECTION);
            conn.out.write(sb.toString().getBytes());
            conn.out.flush();

            String s = conn.in.readLine();
            // DELETED | NOT_DELETED | NOT_FOUND | SERVER_ERROR
            if (s.startsWith("DELETED")) {
                return true;
            } else if (s.startsWith("NOT_DELETED")) {
                return false;
            } else if (s.startsWith("NOT_FOUND")) {
                return false;
            } else if (s.startsWith("SERVER_ERROR")
        	    || s.startsWith("CLIENT_ERROR")
        	    || s.startsWith("ERROR")) {
                throw new ClientException(s);
            } else {
                return false;
            }
        } catch (IOException e) {
            throw new ClientException(e);
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
