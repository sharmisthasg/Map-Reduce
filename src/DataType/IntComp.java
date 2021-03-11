package DataType;

public class IntComp implements Comparable<IntComp> {

    private int value;

    public IntComp(int value){
        this.value=value;
    }
    public IntComp(){
        this.value = 0;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "IntComp{" +
                "value=" + value +
                '}';
    }

    @Override
    public int compareTo(IntComp intComp) {
        return this.value - intComp.getValue();
    }
}
