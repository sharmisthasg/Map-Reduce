package Service;

import Model.Output;

public interface UDFInterface<MAP_KEY_IN, MAP_VALUE_IN, REDUCE_KEY_IN, REDUCE_VALUE_IN> {
    public void map(MAP_KEY_IN key, MAP_VALUE_IN value, Output output);
    public void reduce(REDUCE_KEY_IN key, Iterable<REDUCE_VALUE_IN> valueIter, Output output);
}
