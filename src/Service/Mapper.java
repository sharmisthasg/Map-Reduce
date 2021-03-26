package Service;

import Constants.MRConstant;
import DataType.IntComp;
import DataType.KeyValuePair;
import DataType.StringComp;
import Model.Output;
import Model.WorkerStatus;
import Nodes.Worker;

import java.io.*;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class Mapper implements MRService{

    private int id;
    private int ioPort;
    private String workerType;
    private List<String> inputFilePath;
    private String udfClass;
    private String OUTPUT_PATH = "../../tmp";

    //Tejas:
    private ArrayList<KeyValuePair> kvpairs;
    private int offset;
    private int startLine;


    public Mapper(int id, String workerType, int ioPort, List<String> inputFilePath, String udfClass) {
        this.id = id;
        this.ioPort = ioPort;
        this.workerType = workerType;
        this.inputFilePath = inputFilePath;
        this.udfClass = udfClass;
    }

    public List<List> readFile()
    {
        List<Integer> doc_ids = new ArrayList<>();
        List<String> combined_data = new ArrayList<>();
        try {
            int doc_id=0;
            for (String filepath : inputFilePath) {
                File inputFile = new File(filepath);
                Scanner sc = new Scanner(inputFile);
                while (sc.hasNextLine()) {
                    String data = sc.nextLine();
                    combined_data.add(data);
                    doc_ids.add(doc_id);
                    doc_id++;
                }
                sc.close();
            }
        }catch (Exception e)
        {
            e.printStackTrace();
        }
        List<List> r = new ArrayList<>();
        r.add(doc_ids);
        r.add(combined_data);
        return r;
    }

    public Output getMapped(Output output,int doc_id,String data)
    {
        try{
            Class cls = Class.forName("TestCases."+udfClass);
            Class args[] = new Class[3];
            args[0] = StringComp.class;
            args[1] = StringComp.class;
            args[2] = Output.class;

            Method map_method = cls.getDeclaredMethod("map", args);

            map_method.invoke(cls.newInstance(), new StringComp(String.valueOf(doc_id)), new StringComp(data), output);

        }catch (Exception e){
            e.printStackTrace();
        }
        return output;
    }

    @Override
    public void execute() throws FileNotFoundException, NoSuchMethodException {
        System.out.println("Mapper Process Started");
        try
        {
            Socket socket = new Socket("127.0.0.1", this.ioPort);
            System.out.println("Connected to Server");
            // sends output to the socket
            ObjectOutputStream out    = new ObjectOutputStream(socket.getOutputStream());
            List<List> data = readFile();
            List<Integer> doc_ids = data.get(0);
            List<String> combined_data = data.get(1);
            Output output = new Output();
            for(int i=0; i<doc_ids.size(); i++)
            {
                output = getMapped(output,doc_ids.get(i),combined_data.get(i));
            }
            System.out.println(output);
            String output_filename = write(output);
            WorkerStatus workerStatus = new WorkerStatus(output_filename, MRConstant.SUCCESS, id);
            out.writeObject(workerStatus);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public String write(Output output)
    {
        try {
            String filename = "intermediate-mapper-"+String.valueOf(startLine)+"-"+String.valueOf(offset)+".txt";
            FileWriter fileWriter = new FileWriter("intermediate/"+filename);

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

    @Override
    public String toString() {
        return "Mapper{" +
                "id=" + id +
                ", ioPort=" + ioPort +
                ", workerType='" + workerType + '\'' +
                ", inputFilePath=" + inputFilePath +
                ", udfClass='" + udfClass + '\'' +
                ", OUTPUT_PATH='" + OUTPUT_PATH + '\'' +
                ", kvpairs=" + kvpairs +
                ", offset=" + offset +
                ", startLine=" + startLine +
                '}';
    }
}
