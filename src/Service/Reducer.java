package Service;

import Constants.MRConstant;
import CustomException.MapReduceException;
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
    private String outputFilePath;
    private int numberOfWorkers;
    private boolean forceWorkerException;
    private boolean forceWorkerCrash;
    private int nodeToCrash;

    public Reducer(int id, String workerType, int ioPort, List<String> inputFilePath,
                   String udfClass, String outputFilePath, String numberOfWorkers,int nodeToCrash, boolean forceWorkerCrash, boolean forceWorkerException) {
        this.id = id;
        this.ioPort = ioPort;
        this.workerType = workerType;
        this.inputFilePath = inputFilePath;
        this.udfClass = udfClass;
        this.keys = new ArrayList<>();
        this.outputFilePath=outputFilePath;
        this.numberOfWorkers = Integer.parseInt(numberOfWorkers);
        this.nodeToCrash = nodeToCrash;
        this.forceWorkerCrash = forceWorkerCrash;
        this.forceWorkerException = forceWorkerException;
    }

    @Override
    public void execute() throws IOException {
        System.out.println("Reducer Process Started. ID: "+String.valueOf(id));
        Socket socket = new Socket("127.0.0.1", this.ioPort);
        System.out.println(String.valueOf(id) + ": Connected to Server");
        try {
            if(this.forceWorkerCrash && this.id == this.nodeToCrash){
                System.out.println("Will be crashing this node: " + this.id + " as the node selected to crash for testing is: " + this.nodeToCrash);
                System.exit(0);
            }else if(this.forceWorkerException && this.id == this.nodeToCrash){
                System.out.println("Will be throwing exception for this node: " + this.id + " as the node selected to throw exception for testing is: " + this.nodeToCrash);
                throw new MapReduceException("Forced Exception to Simulate Fault");
            }
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            if(inputFilePath.isEmpty()){
                WorkerStatus workerStatus = new WorkerStatus(null, MRConstant.SUCCESS, id);
                out.writeObject(workerStatus);
                return;
            }
            for(String filename: inputFilePath)
            {
                File inputFile = new File(filename);
                Scanner sc = new Scanner(inputFile);
                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    line = line.replace("<","");
                    line = line.replace(">","");
                    String[] data = line.split(",");
                    if(!keys.contains(data[0]))
                    {
                        keys.add(data[0]);
                    }
                }
            }
            //Reading data from the different input files received from the Master
            HashMap<String,List<StringComp>> combined_data=new HashMap<String,List<StringComp>>();
            for(String filename: inputFilePath)
            {
                File inputFile = new File(filename);
                Scanner sc = new Scanner(inputFile);
                while (sc.hasNextLine())
                {
                    String line = sc.nextLine();
                    line = line.replace("<","");
                    line = line.replace(">","");
                    String[] data = line.split(",");
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

            //Using Java Reflection to invoke the UDF Classes
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
            TreeMap<StringComp, StringComp> sortedOutput = sortOutputKeys(output);

            //Reducer is writing to one output file with the workerId as the file name
            write(sortedOutput);
            System.out.println(String.valueOf(id) + ": Reducer has written to Output Files");
            WorkerStatus workerStatus = new WorkerStatus(null, MRConstant.SUCCESS, id);
            out.writeObject(workerStatus);

        } catch (Exception e) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            WorkerStatus workerStatus = new WorkerStatus(null, MRConstant.FAILURE, id);
            out.writeObject(workerStatus);
            e.printStackTrace();
        }
    }

    private TreeMap<StringComp, StringComp> sortOutputKeys(Output output) {
        TreeMap<StringComp, StringComp> sortedOutput = new TreeMap<>();
        for(Map.Entry<Object,Object> entry : output.getOutputMap().entrySet()){
            StringComp key = (StringComp) entry.getKey();
            StringComp value = (StringComp) entry.getValue();
            sortedOutput.put(key,value);
        }
        return sortedOutput;
    }

    public void write(TreeMap<StringComp, StringComp> sortedOutput) {
        String outputFileName = buildOutputFilePath();
        try {
            FileWriter fileWriter = new FileWriter(outputFileName);
            for (Map.Entry<StringComp,StringComp> entry : sortedOutput.entrySet())
            {
                StringComp key = entry.getKey();
                StringComp value = entry.getValue();
                String output_write = key.getValue()+" "+value.getValue();
                output_write = output_write.trim();
                fileWriter.write(output_write);
                fileWriter.write("\n");
            }
            fileWriter.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private String buildOutputFilePath() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.outputFilePath);
        sb.append("/");
        sb.append(this.udfClass);
        sb.append("/");
        sb.append(this.id);
        sb.append(".txt");
        return sb.toString();
    }
}
