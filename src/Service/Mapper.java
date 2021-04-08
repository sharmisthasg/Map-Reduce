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
import java.sql.SQLOutput;
import java.util.*;

public class Mapper implements MRService{

    private int id;
    private int ioPort;
    private String workerType;
    private List<String> inputFilePath;
    private String udfClass;
    private String OUTPUT_PATH = "../../tmp";
    private int offset;
    private int startLine;
    private int numberOfWorkers;


    public Mapper(int id, String workerType, int ioPort, List<String> inputFilePath, String udfClass, String startLine, String offset, String numberOfWorkers) {
        this.id = id;
        this.ioPort = ioPort;
        this.workerType = workerType;
        this.inputFilePath = inputFilePath;
        this.udfClass = udfClass;
        this.startLine=Integer.parseInt(startLine);
        this.offset=Integer.parseInt(offset);
        this.numberOfWorkers = Integer.parseInt(numberOfWorkers);
    }

    public List<List> readFile()
    {
        //TODO: Startline and offset
        List<Integer> doc_ids = new ArrayList<>();
        List<String> combined_data = new ArrayList<>();
        try {
            int doc_id=0;
            for (String filepath : inputFilePath) {
                File inputFile = new File(filepath);
                Scanner sc = new Scanner(inputFile);
                int line_number = 0;
                while (sc.hasNextLine()) {
                    String data = sc.nextLine();
                    if(line_number>=startLine && line_number<(startLine+offset)){
                        data = preprocess(data);
                        combined_data.add(data);
                        doc_ids.add(doc_id);
                        doc_id++;
                    }
                    line_number++;
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
            //TODO: Create a Map with Key as Reducer id in [0,numberOfWorkers-1] and output_filename as the value.
            HashMap<String,String> output_map = write(output);

            System.out.println("Written to intermediate File");
            WorkerStatus workerStatus = new WorkerStatus(output_map, MRConstant.SUCCESS, id);
            out.writeObject(workerStatus);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public HashMap<String,String> write(Output output)
    {
        try {

            HashMap<String,String> output_map=new HashMap<String,String>();//Creating HashMap
            String filename = udfClass+"/"+String.valueOf(id)+"-"+String.valueOf(offset);
            //FileWriter fileWriter = new FileWriter("intermediate/"+filename);

            Map<Object, Object> outputMap = output.getOutputMap();
            for (Map.Entry<Object,Object> entry : outputMap.entrySet())
            {
                StringComp key = (StringComp) entry.getKey();
                StringComp value = (StringComp) entry.getValue();
                int hashkey = hashKey(key.getValue());

<<<<<<< HEAD
                String filepath = "intermediate/"+filename + "-" + String.valueOf(hashkey)+".txt";
                FileWriter fw = new FileWriter(filepath);
=======
                String filepath = "intermediate/"+filename+"-"+String.valueOf(hashkey)+".txt";
                FileWriter fw = new FileWriter(filepath,true);
>>>>>>> 35cae35f40690c6ebdf1110a9c2396850477086d
                fw.write("<"+key.getValue()+","+value.getValue()+">\n");
                fw.close();
                output_map.put(String.valueOf(hashkey),filepath);
            }
            return output_map;
        }catch (Exception e){
            e.printStackTrace();
            return new HashMap<String,String>();
        }
    }

    private String preprocess(String data){
        return data.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase();
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
                ", offset=" + offset +
                ", startLine=" + startLine +
                '}';
    }

    public int hashKey(String key){

        int keysum=0;
        for(int i=0;i<key.length();i++)
        {
            keysum+=Character.getNumericValue(key.charAt(i));
        }

        return keysum%numberOfWorkers;
    }
}
