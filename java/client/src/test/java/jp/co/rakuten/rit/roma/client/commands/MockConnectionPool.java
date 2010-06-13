package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;

import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.ConnectionPool;
import jp.co.rakuten.rit.roma.client.Node;

public class MockConnectionPool implements ConnectionPool {

    public void closeAll() {
    }

    public void delete(Node node, Connection conn) {
    }

    public void deleteAll(Node node) {
    }

    public Connection get(Node node) throws IOException {
	return null;
    }

    public void put(Node node, Connection conn) throws IOException {
    }
}