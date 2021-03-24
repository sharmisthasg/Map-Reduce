package Service;

import Constants.MRConstant;
import DataType.IntComp;
import DataType.StringComp;
import Model.Output;
import Model.WorkerStatus;

import java.io.*;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Reducer implements MRService {

    private int id;
    private int ioPort;
    private String workerType;
    private List<String> inputFilePath;
    private List<String> keys;
    private String udfClass;

    public Reducer(int id, String workerType, int ioPort, List<String> inputFilePath,
                   String udfClass) {
        this.id = id;
        this.ioPort = ioPort;
        this.workerType = workerType;
        this.inputFilePath = inputFilePath;
        this.udfClass = udfClass;
        this.keys = new ArrayList<>();
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

            for(String filename: inputFilePath)
            {
                File inputFile = new File("intermediate/"+filename);
                Scanner sc = new Scanner(inputFile);
                while (sc.hasNextLine()) {
                    String[] data = sc.nextLine().split(" ");
                    if(!keys.contains(data[0]))
                    {
                        keys.add(data[0]);
                    }
                }
            }

            HashMap<String,List<StringComp>> combined_data=new HashMap<String,List<StringComp>>();
            for(String filename: inputFilePath)
            {
                File inputFile = new File("intermediate/"+filename);
                Scanner sc = new Scanner(inputFile);
                while (sc.hasNextLine())
                {
                    String temp = sc.nextLine();
                    String[] data = temp.split(" ");
                    String curr_key = data[0];
                    String curr_value = data[1];

                    if(keys.contains(curr_key))
                    {
                        if(combined_data.containsKey(curr_key))
                        {
                            List<StringComp> orig = combined_data.get(curr_key);
                            orig.add(new StringComp(curr_value));
                            combined_data.put(curr_key,orig);
                        }
                        else
                        {
                            List<StringComp> vals=new ArrayList<StringComp>();
                            vals.add(new StringComp(curr_value));
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

            for(String key: combined_data.keySet())
            {
                List<StringComp> values = combined_data.get(key);
                map_method.invoke(cls.newInstance(), new StringComp(key), values, output);
            }

            //System.out.println(output);
            String output_filename = write(output);
            WorkerStatus workerStatus = new WorkerStatus(output_filename, MRConstant.SUCCESS, id);
            out.writeUTF(output_filename);
            out.writeUTF("Reducer complete");
            out.writeUTF("Over");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String write(Output output)
    {
        try {
            String filename = "output-reducer-"+String.valueOf(id)+".txt";
            FileWriter fileWriter = new FileWriter("output/"+filename);

            Map<Object, Object> outputMap = output.getOutputMap();

            for (Map.Entry<Object,Object> entry : outputMap.entrySet())
            {
                StringComp key = (StringComp)entry.getKey();
                IntComp value = (IntComp) entry.getValue();
                fileWriter.write(key.getValue()+" "+value.getValue()+"\n");
            }
            fileWriter.close();
            return filename;
        }catch (Exception e){
            e.printStackTrace();
            return "";
        }
    }
}
