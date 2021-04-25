package Service;

import Constants.MRConstant;
import CustomException.MapReduceException;
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
    private int offset;
    private int startLine;
    private int numberOfWorkers;
    private boolean forceWorkerException;
    private boolean forceWorkerCrash;
    private int nodeToCrash;


    public Mapper(int id, String workerType, int ioPort,
                  List<String> inputFilePath, String udfClass,
                  String startLine, String offset, String numberOfWorkers,
                  int nodeToCrash, boolean forceWorkerCrash, boolean forceWorkerException) {
        //instantiate mapper process with parameters
        this.id = id;
        this.ioPort = ioPort;
        this.workerType = workerType;
        this.inputFilePath = inputFilePath;
        this.udfClass = udfClass;
        this.startLine=Integer.parseInt(startLine);
        this.offset=Integer.parseInt(offset);
        this.numberOfWorkers = Integer.parseInt(numberOfWorkers);
        this.nodeToCrash = nodeToCrash;
        this.forceWorkerCrash = forceWorkerCrash;
        this.forceWorkerException = forceWorkerException;
    }

    /*
    This function reads the lines in the file between the startline and startline + offset value
     */
    public List<List> readFile()
    {
        //read input file to mapper process
        List<Integer> doc_ids = new ArrayList<>();
        List<String> combined_data = new ArrayList<>();
        try {
            int doc_id=startLine;
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
        //invoke the UDF for mapper
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
    public void execute() throws IOException, NoSuchMethodException {
        System.out.println("Mapper Process Started. ID: "+String.valueOf(id));
        //Establish socket communication
        Socket socket = new Socket("127.0.0.1", this.ioPort);
        System.out.println(String.valueOf(id) + ": Connected to Server");
        try {
            //Checks for fault tolerance
            if(this.forceWorkerCrash && this.id == this.nodeToCrash){
                System.out.println("Will be crashing this node: " + this.id + " as the node selected to crash for testing is: " + this.nodeToCrash);
                System.exit(0);
            }else if(this.forceWorkerException && this.id == this.nodeToCrash){
                System.out.println("Will be throwing exception for this node: " + this.id + " as the node selected to throw exception for testing is: " + this.nodeToCrash);
                throw new MapReduceException("Forced Exception to Simulate Fault");
            }
            System.out.println(String.valueOf(id) + ": Connected to Server");
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
            HashMap<String,String> output_map = write(output);

            System.out.println(String.valueOf(id) + ": Written to intermediate File");
            WorkerStatus workerStatus = new WorkerStatus(output_map, MRConstant.SUCCESS, id);
            out.writeObject(workerStatus);
        }catch (Exception e){
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            WorkerStatus workerStatus = new WorkerStatus(null, MRConstant.FAILURE, id);
            out.writeObject(workerStatus);
            e.printStackTrace();
        }

    }

    public HashMap<String,String> write(Output output)
    {
        try {
            //write mapper output to intermediate files
            HashMap<String,String> output_map=new HashMap<String,String>();//Creating HashMap
            String filename = udfClass+"/"+String.valueOf(id)+"-"+String.valueOf(offset);
            //FileWriter fileWriter = new FileWriter("intermediate/"+filename);

            Map<Object, Object> outputMap = output.getOutputMap();
            for (Map.Entry<Object,Object> entry : outputMap.entrySet())
            {
                StringComp key = (StringComp) entry.getKey();
                StringComp value = (StringComp) entry.getValue();
                int hashkey = hashKey(key.getValue());
                String filepath = "intermediate/"+filename + "-" + String.valueOf(hashkey)+".txt";
                FileWriter fw = new FileWriter(filepath,true);
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
        // generate hash values for given key
        int keysum=0;
        for(int i=0;i<key.length();i++)
        {
            keysum+=Character.getNumericValue(key.charAt(i));
        }

        return keysum%numberOfWorkers;
    }
}
