package jp.co.rakuten.rit.roma.client.commands;

import jp.co.rakuten.rit.roma.client.ClientException;

public class RetryOutException extends ClientException {
  public RetryOutException() {
    super("Retry out");
  }

  public RetryOutException(String reason) {
    super(reason);
  }

  public RetryOutException(Throwable t) {
    super(t);
  }

  private static final long serialVersionUID = 2963394216686818649L;
}