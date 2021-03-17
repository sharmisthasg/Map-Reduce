package Nodes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

class Solution {
    public static void main(String[] args) throws Exception
    {
        System.out.println("Hello World");
        Runner r = new Runner(5);
        r.run();

        ProcessBuilder pb = new ProcessBuilder(new String[]{"java", "/Desktop/Solution.java"});
        Process pro = pb.start();

        pro.destroy();
    }
}

class Runner {
    private final int n;

    public Runner(int n)
    {
        this.n=n;
    }

    public void run()
    {
        for(int i=1;i<=n;i++)
        {
            System.out.println(i);
        }
    }
}