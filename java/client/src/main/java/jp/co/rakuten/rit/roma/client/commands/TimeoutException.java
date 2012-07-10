package jp.co.rakuten.rit.roma.client.commands;

import jp.co.rakuten.rit.roma.client.ClientException;

public class TimeoutException extends ClientException {
  private static final long serialVersionUID = 1262780445524677010L;

  public TimeoutException(Throwable t) {
    super(t);
  }
}