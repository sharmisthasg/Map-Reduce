package Service;

import DataType.StringComp;
import Model.Output;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class Reducer implements MRService {

    private int id;
    private int ioPort;
    private String workerType;
    private List<String> inputFilePath;
    private List<StringComp> keys;
    private String udfClass;

    public Reducer(int id, String workerType, int ioPort, List<String> inputFilePath,
                   String udfClass, List<StringComp> keys) {
        this.id = id;
        this.ioPort = ioPort;
        this.workerType = workerType;
        this.inputFilePath = inputFilePath;
        this.udfClass = udfClass;
        this.keys = keys;
    }

    @Override
    public void execute() {
        System.out.println("Reducer Process Started");

        try {
            Socket socket = new Socket("127.0.0.1", this.ioPort);
            System.out.println("Connected");

            // takes input from terminal
            DataInputStream in = new DataInputStream(System.in);

            // sends output to the socket
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeUTF("Starting communication with Reducer");


            HashMap<StringComp,List<StringComp>> combined_data=new HashMap<StringComp,List<StringComp>>();
            File folderpath = new File("intermediate/");
            for(File filename: folderpath.listFiles())
            {
                File inputFile = new File("intermediate/"+filename);
                Scanner sc = new Scanner(inputFile);
                while (sc.hasNextLine())
                {
                    String[] data = sc.nextLine().split(" ");
                    StringComp curr_key = new StringComp(data[0]);
                    StringComp curr_value = new StringComp(data[1]);

                    if(keys.contains(curr_key))
                    {
                        if(combined_data.containsKey(curr_key))
                        {
                            List<StringComp> orig = combined_data.get(curr_key);
                            orig.add(curr_value);
                            combined_data.put(curr_key,orig);
                        }
                        else
                        {
                            List<StringComp> vals=new ArrayList<>();
                            vals.add(curr_value);
                            combined_data.put(curr_key,vals);
                        }
                    }
                }
                sc.close();
            }

            Class cls = Class.forName("TestCases."+udfClass);
            Class args[] = new Class[3];
            args[0] = StringComp.class;
            args[1] = Iterable.class;
            args[2] = Output.class;

            Method map_method = cls.getDeclaredMethod("reduce", args);
            Output output = new Output();

            for(StringComp key: combined_data.keySet())
            {
                List<StringComp> values = combined_data.get(key);
                map_method.invoke(cls.newInstance(), key, values, output);
            }

            out.writeUTF("Reducer complete");
            out.writeUTF("Over");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
