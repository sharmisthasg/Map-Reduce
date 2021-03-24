package Service;

import Constants.MRConstant;
import DataType.IntComp;
import DataType.KeyValuePair;
import DataType.StringComp;
import Model.Output;

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

    @Override
    public void execute() throws FileNotFoundException, NoSuchMethodException {
        /*
        1. load file using inputFilePath
        2. docId, String {entire document)
        3. Using java reflection call
         */
        System.out.println("Mapper Process Started");


        try
        {
            Socket socket = new Socket("127.0.0.1", this.ioPort);
            System.out.println("Connected");

            // takes input from terminal
            DataInputStream in  = new DataInputStream(System.in);

            // sends output to the socket
            DataOutputStream out    = new DataOutputStream(socket.getOutputStream());
            out.writeUTF("Starting communication with Mapper");


        String combined_data="";


        for(String filepath: inputFilePath)
        {
            File inputFile = new File(filepath);
            Scanner sc = new Scanner(inputFile);
            while (sc.hasNextLine()) {
                String data = sc.nextLine();
                combined_data = combined_data + " " + data;
            }
            sc.close();
        }

            Class cls = Class.forName("TestCases."+udfClass);
            Class args[] = new Class[3];
            args[0] = StringComp.class;
            args[1] = StringComp.class;
            args[2] = Output.class;

            Method map_method = cls.getDeclaredMethod("map", args);

            Output output = new Output();
            map_method.invoke(cls.newInstance(), new StringComp("1"), new StringComp(combined_data), output);
            String output_filename = write(output);
            out.writeUTF(output_filename);
            out.writeUTF("Over");

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
