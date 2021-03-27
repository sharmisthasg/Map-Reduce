package TestCases;

import DataType.StringComp;
import Model.Output;
import Service.UDFInterface;

public class DistributedGrep implements UDFInterface<StringComp, StringComp, StringComp, StringComp>
{
    String pattern="map";
    @Override
    public void map(StringComp key, StringComp value, Output output) {

        StringComp res=value;
        if(value.getValue().contains(pattern))
        {
            output.write(res, new StringComp("1"));
        }
        else
        {
            output.write(res, new StringComp("0"));
        }

    }

    @Override
    public void reduce(StringComp key, Iterable<StringComp> valueIter, Output output) {
        for(StringComp s:valueIter)
        {
            if(s.getValue().equals("1"))
            {
                output.write(key,new StringComp(""));
            }
        }

    }
}
