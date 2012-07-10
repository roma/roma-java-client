package jp.co.rakuten.rit.roma.client.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.RomaClient;

/**
 * Specification of data format
 * 
 * key: original key value: [magic number][unique count][value len][segs
 * len][each val len[]]
 * 
 * each key: [original key][:][unigue count][:][key num] each value: segment of
 * value
 */
public class DataSplitWrapper {
  private static final int DEFAULT_SPLITING_SIZE = 1024;

  protected RomaClient client;

  protected int size;

  private static byte[] MAGIC_NUMBER = new byte[] { 0x36, 0x32, 0x36, 0x31,
      0x36, 0x63, 0x37, 0x33, 0x36, 0x35 };

  public DataSplitWrapper(RomaClient client) {
    this(client, DEFAULT_SPLITING_SIZE);
  }

  public DataSplitWrapper(RomaClient client, int size) {
    this.client = client;
    this.size = size;
  }

  public boolean put(final String key, byte[] bytes, long expiry)
      throws ClientException {
    if (key == null) {
      throw new NullPointerException("key is null.");
    }
    if (expiry == 0) {
      throw new ClientException("Cannot specify zero as an expire time.");
    }

    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      DataOutputStream dout = new DataOutputStream(out);

      // magic number
      dout.write(MAGIC_NUMBER);

      // unique count
      long uniqueCount = System.currentTimeMillis();
      dout.writeLong(uniqueCount);

      // value length
      int len = bytes.length;
      dout.writeInt(len);
      List<Integer> valLens = new ArrayList<Integer>();
      int offset = 0;
      int num = 0;
      while (offset < len) {
        byte[] b;
        if ((len - offset) - size >= 0) {
          b = new byte[size];
        } else {
          b = new byte[len - offset];
        }
        System.arraycopy(bytes, offset, b, 0, b.length);
        offset = offset + b.length;
        String k = key + ":" + uniqueCount + ":" + num;
        valLens.add(b.length);
        boolean ret = client.put(k, b, expiry);
        if (!ret) {
          return false;
        }
        num++;
      }

      // number of segments
      dout.writeInt(num);

      // size of each segment
      for (int valLen : valLens) {
        dout.writeInt(valLen);
      }
      dout.flush();
      byte[] value = out.toByteArray();
      return client.put(key, value, expiry);
    } catch (IOException e) {
      throw new ClientException(e);
    }
  }

  public byte[] get(final String key) throws ClientException {
    if (key == null) {
      throw new NullPointerException("key is null.");
    }

    try {
      byte[] b = client.get(key);
      if (b == null || b.length <= MAGIC_NUMBER.length) {
        return null;
      }

      ByteArrayInputStream in = new ByteArrayInputStream(b);
      DataInputStream din = new DataInputStream(in);

      // magic number
      byte[] mn = new byte[MAGIC_NUMBER.length];
      din.read(mn);
      if (!isChunkData(mn)) {
        return b;
      }

      // unique count
      long uniqueCount = din.readLong();

      // value length
      int len = din.readInt();
      byte[] bytes = new byte[len];

      // number of segments
      int num = din.readInt();

      // size of each segment
      int[] valLens = new int[num];
      for (int i = 0; i < num; ++i) {
        valLens[i] = din.readInt();
      }

      int offset = 0;
      for (int i = 0; i < num; ++i) {
        String k = key + ":" + uniqueCount + ":" + i;
        byte[] v = client.get(k);
        if (v == null || v.length != valLens[i]) {
          throw new ClientException("segmentation exception: key: " + k);
        }
        System.arraycopy(v, 0, bytes, offset, v.length);
        offset = offset + v.length;
      }
      return bytes;
    } catch (IOException e) {
      throw new ClientException(e);
    }
  }

  public boolean delete(final String key) throws ClientException {
    if (key == null) {
      throw new NullPointerException();
    }

    try {
      byte[] b = client.get(key);
      if (b == null || b.length <= MAGIC_NUMBER.length) {
        return false;
      }

      ByteArrayInputStream in = new ByteArrayInputStream(b);
      DataInputStream din = new DataInputStream(in);

      // magic number
      byte[] mn = new byte[MAGIC_NUMBER.length];
      din.read(mn);
      if (!isChunkData(mn)) {
        return client.delete(key);
      }

      // unique count
      long uniqueCount = din.readLong();

      // value length
      din.readInt();

      // number of segments
      int num = din.readInt();

      // size of each segment
      int[] valLens = new int[num];
      for (int i = 0; i < num; ++i) {
        valLens[i] = din.readInt();
      }

      boolean ret = client.delete(key);
      for (int i = 0; i < num; ++i) {
        try {
          String k = key + ":" + uniqueCount + ":" + i;
          client.delete(k);
        } catch (ClientException e) { // ignore
        }
      }
      return ret;
    } catch (IOException e) {
      throw new ClientException(e);
    }
  }

  private static boolean isChunkData(byte[] mn) {
    for (int i = 0; i < MAGIC_NUMBER.length; ++i) {
      if (MAGIC_NUMBER[i] != mn[i]) {
        return false;
      }
    }
    return true;
  }
}