package jp.co.rakuten.rit.roma.client;

/**
 * A value with a CAS identifier.
 */
public class CasValue {
    private final long cas;
    private final byte[] value;

    /**
     * Construct a new CASValue with the given identifer and value.
     *
     * @param c the CAS identifier
     * @param v the value
     */
    public CasValue(long c, byte[] v) {
        super();
        cas=c;
        value=v;
    }

    /**
     * Get the CAS identifier.
     */
    public long getCas() {
        return cas;
    }

    /**
     * Get the object value.
     */
    public byte[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "{CasValue " + cas + "/" + value + "}";
    }

}
