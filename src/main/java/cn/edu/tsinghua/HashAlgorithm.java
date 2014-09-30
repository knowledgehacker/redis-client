package cn.edu.tsinghua;

/**
 * Created by minglin on 14-9-30.
 */
public abstract class HashAlgorithm {
    public static final HashAlgorithm PLAIN_HASH_ALGORITHM = new PlainHashAlgorithm();

    public int hash(String key, int size) {
        return hash(key) % size;
    }

    public int hash(byte[] key, int size) {
        return hash(key) % size;
    }

    public abstract int hash(String key);
    public abstract int hash(byte[] key);
}
