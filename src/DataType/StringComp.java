package DataType;

public class StringComp implements Comparable<StringComp>{

    private String value;

    public StringComp(String value) {
        this.value = value;
    }

    public StringComp() {
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "StringComp{" +
                "value='" + value + '\'' +
                '}';
    }

    @Override
    public int compareTo(StringComp stringComp) {
        return this.value.compareTo(stringComp.getValue());
    }
}
