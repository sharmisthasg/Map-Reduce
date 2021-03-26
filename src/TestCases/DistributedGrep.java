package TestCases;

import DataType.StringComp;
import Model.Output;
import Service.UDFInterface;

public class DistributedGrep implements UDFInterface<StringComp, StringComp, StringComp, StringComp>
{
    String pattern="";
    @Override
    public void map(StringComp stringComp, StringComp stringComp2, Output output) {

    }

    @Override
    public void reduce(StringComp stringComp, Iterable<StringComp> valueIter, Output output) {

    }
}
