package TestCases;

import DataType.IntComp;
import DataType.StringComp;
import Model.Output;
import Service.UDFInterface;

import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class InvertedIndex implements UDFInterface<StringComp, StringComp, StringComp, StringComp> {

    @Override
    public void map(StringComp key, StringComp value, Output output) {
        String text = value.getValue();
        StringTokenizer itr = new StringTokenizer(text);
        while (itr.hasMoreTokens()) {
            StringComp word = new StringComp();
            word.setValue(itr.nextToken().trim());
            output.write(word, key);
        }
    }

    @Override
    public void reduce(StringComp key, Iterable<StringComp> valueIter, Output output) {
        Set<String> res_set = new HashSet<>();

        for (StringComp val : valueIter) {
            res_set.add(val.getValue());
        }

        String result_concat="";
        for(String s: res_set)
        {
            result_concat = result_concat + s + ",";
        }
        result_concat=result_concat.substring(0,result_concat.length()-1);
        output.write(key, new StringComp(result_concat));
    }
}
