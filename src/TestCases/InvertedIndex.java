package TestCases;

import DataType.IntComp;
import DataType.StringComp;
import Model.Output;
import Service.UDFInterface;

import java.util.StringTokenizer;

public class InvertedIndex implements UDFInterface<StringComp, StringComp, StringComp, StringComp> {

    @Override
    public void map(StringComp key, StringComp value, Output output) {
        String text = value.getValue();
        StringTokenizer itr = new StringTokenizer(text);
        while (itr.hasMoreTokens()) {
            StringComp word = new StringComp();
            word.setValue(itr.nextToken().trim());
            IntComp one = new IntComp(Integer.parseInt(key.getValue()));
            output.write(word, one);
        }
    }

    @Override
    public void reduce(StringComp key, Iterable<StringComp> valueIter, Output output) {
        StringComp result = new StringComp();
        String result_concat="";
        for (StringComp val : valueIter) {
            result_concat = result + val.getValue() + ",";
        }
        result.setValue(result_concat);
        output.write(key, result);
    }
}
