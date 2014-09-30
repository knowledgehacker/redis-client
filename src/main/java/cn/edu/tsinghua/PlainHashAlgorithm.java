package cn.edu.tsinghua;

/**
 * Created by minglin on 14-9-30.
 */
public class PlainHashAlgorithm extends HashAlgorithm {
    public int hash(String key) {
        return key.hashCode();
    }

    public int hash(byte[] key) {
        return key.hashCode();
    }
}
