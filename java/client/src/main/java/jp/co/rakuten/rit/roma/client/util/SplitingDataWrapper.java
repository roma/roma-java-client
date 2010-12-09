package jp.co.rakuten.rit.roma.client.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.RomaClient;

public class SplitingDataWrapper {
	private static Logger LOG = LoggerFactory
			.getLogger(SplitingDataWrapper.class);

	private static final int DEFAULT_SPLITING_SIZE = 1024;

	protected RomaClient client;

	protected int size;

	private static final byte[] MAGIC_NUMBER = new byte[] { 0x01, 0x02, 0x03,
			0x04 };

	public SplitingDataWrapper(RomaClient client) {
		this(client, DEFAULT_SPLITING_SIZE);
	}

	public SplitingDataWrapper(RomaClient client, int size) {
		this.client = client;
		this.size = size;
	}

	public boolean put(final String key, byte[] bytes, long expiry)
			throws ClientException {
		if (key == null) {
			throw new NullPointerException("key is null.");
		}

		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			DataOutputStream dout = new DataOutputStream(out);
			dout.write(MAGIC_NUMBER);
			int len = bytes.length;
			dout.writeInt(len);
			List<String> keys = new ArrayList<String>();
			List<Integer> valLens = new ArrayList<Integer>();
			long time = new Date().getTime();
			int offset = 0;
			int num = 0;
			while (offset < len) {
				byte[] b;
				if (len - size >= 0) {
					b = new byte[size];
				} else {
					b = new byte[len];
				}
				System.arraycopy(bytes, offset, b, 0, b.length);
				offset = offset + b.length;
				String k = key + ":" + num + ":" + time;
				keys.add(k);
				valLens.add(b.length);
				boolean ret = client.put(k, b, expiry);
				if (!ret) {
					return false;
				}
				num++;
			}
			dout.writeInt(num);
			for (String k : keys) {
				dout.writeInt(k.length());
			}
			for (String k : keys) {
				dout.write(k.getBytes("UTF-8"));
			}
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
			if (b == null) {
				return null;
			}

			ByteArrayInputStream in = new ByteArrayInputStream(b);
			DataInputStream din = new DataInputStream(in);
			byte[] mn = new byte[4];
			din.read(mn);
			if (MAGIC_NUMBER[0] != mn[0] || MAGIC_NUMBER[1] != mn[1]
					|| MAGIC_NUMBER[2] != mn[2] || MAGIC_NUMBER[3] != mn[3]) {
				return b;
			}

			int len = din.readInt();
			byte[] bytes = new byte[len];

			int num = din.readInt();
			int[] keyLens = new int[num];
			for (int i = 0; i < num; ++i) {
				keyLens[i] = din.readInt();
			}
			String[] keys = new String[num];
			for (int i = 0; i < num; ++i) {
				byte[] k = new byte[keyLens[i]];
				din.read(k);
				keys[i] = new String(k, "UTF-8");
			}
			int[] valLens = new int[num];
			for (int i = 0; i < num; ++i) {
				valLens[i] = din.readInt();
			}

			int offset = 0;
			for (int i = 0; i < num; ++i) {
				byte[] v = client.get(keys[i]);
				if (v == null || v.length != valLens[i]) {
					throw new ClientException("segmentation exception: key: " + keys[i]);
				}
				System.arraycopy(v, 0, bytes, offset, v.length);
				offset = offset + v.length;
			}
			return bytes;
		} catch (IOException e) {
			throw new ClientException(e);
		}
	}
}
