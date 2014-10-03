package cn.edu.tsinghua;

/**
 * Created by minglin on 14-9-30.
 */
public class PlainHashAlgorithm extends HashAlgorithm {
    public int hash(String key) {
        return Math.abs(key.hashCode());
    }

    public int hash(byte[] key) {
        return Math.abs(key.hashCode());
    }
}
