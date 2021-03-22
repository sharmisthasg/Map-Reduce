package TestCases;

import DataType.IntComp;
import DataType.StringComp;
import Model.Output;
import Service.UDFInterface;
import java.io.IOException;
import java.util.*;
import java.util.StringTokenizer;
import TestCases.WordCount;

public class testwc {
    public static void main(String[] args) {
        StringComp key = new StringComp("doc_1");
        StringComp value = new StringComp("hello hello hi hi whatsup now");
        Output output = new Output();
        WordCount wc = new WordCount();
        wc.map(key,value,output);
        System.out.println(output);

        StringComp key_red = new StringComp("hello");
        IntComp x1 = new IntComp(2);
        IntComp x2 = new IntComp(1);
        Iterable<IntComp> iter = Arrays.asList(x1, x2);
        Output output_red = new Output();
        wc.reduce(key_red, iter, output_red);
        StringComp key_red_1 = new StringComp("hi");
        IntComp x3 = new IntComp(1);
        IntComp x4 = new IntComp(2);
        IntComp x5 = new IntComp(1);
        Iterable<IntComp> iter_1 = Arrays.asList(x3, x4, x5);
        wc.reduce(key_red_1, iter_1, output_red);
        System.out.println(output_red);
    }
}
