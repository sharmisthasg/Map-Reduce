package TestCases;

import DataType.IntComp;
import DataType.StringComp;
import Model.Output;
import Service.UDFInterface;

public class WordCount implements UDFInterface<StringComp, IntComp, StringComp, IntComp> {

    @Override
    public void map(StringComp stringComp, IntComp intComp, Output output) {

    }

    @Override
    public void reduce(StringComp stringComp, Iterable<IntComp> valueIter, Output output) {

    }
}
