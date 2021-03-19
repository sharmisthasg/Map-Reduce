package DataType;

public class KeyValuePair {

    String key;
    String value;

    public KeyValuePair(String key, String value)
    {
        this.key=key;
        this.value=value;
    }

    public String toKeyValueString()
    {
        return "<"+key+","+value+">";
    }

    @Override
    public String toString() {
        return key+" "+value;
    }
}
