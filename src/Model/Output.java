package Model;

import java.util.*;

public class Output {

    //<k,V> pair output will be set here
    private Map<Object,Object> outputMap;

    public Output(){
        outputMap = new HashMap<Object, Object>();
    }

    public void write(Object key, Object value){
        outputMap.put(key,value);
    }

    public Map<Object, Object> getOutputMap() {
        return outputMap;
    }

    @Override
    public String toString() {
        return "Output{" +
                "outputMap=" + outputMap +
                '}';
    }

}
