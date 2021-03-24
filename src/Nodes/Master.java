package Nodes;

import Config.MapReduceProperties;
import Constants.MRConstant;
import Model.WorkerStatus;
import ProcessStates.ActiveWorkers;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.stream.Collectors;

public class Master {

    String numOfWorkers;
    String inputFilePath;
    String outputFilePath;
    String udfClass;
    int ioPort = 5001;

    public Master(){}

    public void start() throws IOException {
        System.out.println("Loading properties from config file....");
        MapReduceProperties mrProp = new MapReduceProperties();
        Properties prop = mrProp.getProperties();
        this.numOfWorkers = prop.getProperty("N");
        this.inputFilePath = prop.getProperty("input_file_path");
        this.outputFilePath = prop.getProperty("output_file_path");
        this.udfClass = prop.getProperty("udf_class");
        System.out.println("Properties Loaded => " + prop.values());
        execute();
    }

    /*int workerId = Integer.parseInt(args[0]);
    int ioPort = Integer.parseInt(args[1]);
    String workerType = args[2];
    String inputFilePathStr = args[3];
    List<String> inputFilePath = Arrays.asList(inputFilePathStr.split(" "));
    String udfClass = args[4];
    String startLine = args[5];
    String offset = args[6];*/

    public void execute(){
        try {
            int numberOfLines = countLinesFile();
            int offset = (int) Math.ceil((double)numberOfLines/(double) Integer.parseInt(this.numOfWorkers));
            int startLine = 0;
            int workerId = 0;
            System.out.println(numberOfLines);
            boolean received = true;
            ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
            while (startLine < numberOfLines) {
                String commandList[] = {"java", "-cp", "out/production/MapReduceProject",
                        MRConstant.WORKER_JAVA_LOCATION,
                        String.valueOf(workerId),
                        String.valueOf(this.ioPort),
                        MRConstant.MAPPER,
                        this.inputFilePath,
                        this.udfClass,
                        String.valueOf(startLine),
                        String.valueOf(offset)
                };
                // creating worker node process with workerType = Mapper
                ProcessBuilder processBuilder = new ProcessBuilder(commandList);
                processBuilder.inheritIO();
                Process process = processBuilder.start();
                activeWorkers.isActiveWorker.add(workerId);
                startLine += offset;
                workerId++;
            }
            ServerSocket server = new ServerSocket(this.ioPort);
            System.out.println("Server started");

            System.out.println("Waiting for a client ...");

            Socket socket = server.accept();
            System.out.println("Client accepted");

            // takes input from the client socket
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            String line = "";
            List<String> reducerInputFiles = new ArrayList<>();
            while (!activeWorkers.isActiveWorker.isEmpty())
            {
                WorkerStatus status = (WorkerStatus) ois.readObject();
                System.out.println("Received ==> " + status);
                if(MRConstant.SUCCESS.equals(status.getStatus())){
                    activeWorkers.isActiveWorker.remove(status.getWorkerId());
                    reducerInputFiles.add(status.getFilePath());

                }
            }

            //Calling Reducer
            String reducerFilesStr = reducerInputFiles.stream().collect(Collectors.joining(","));
            int reducers = 0;
            while (reducers < Integer.parseInt(this.numOfWorkers)) {
                String commandList[] = {"java", "-cp", "out/production/MapReduceProject",
                        MRConstant.WORKER_JAVA_LOCATION,
                        String.valueOf(workerId),
                        String.valueOf(this.ioPort),
                        MRConstant.REDUCER,
                        reducerFilesStr,
                        this.udfClass,
                        "",
                        ""
                };
                // creating worker node process with workerType = Mapper
                ProcessBuilder processBuilder = new ProcessBuilder(commandList);
                processBuilder.inheritIO();
                Process process = processBuilder.start();
                activeWorkers.isActiveWorker.add(workerId);
                workerId++;
            }

        }catch(Exception e){
            e.printStackTrace();
        }
        /*TODO:
    1. Read from config file using Properties which creates a map of sorts
    2. Read number of lines in the file 'K'-> K/N is the offset, startline = startline + offset
    2. Create N mapper (Worker class) processes and update ActiveWorkers
    3. Wait for N mapper process to get completed
    4. While(True) for 2 and 3
    5. On completion create N reducer (Worker class) processes and update ActiveWorkers
    6. Wait for N reducer process to get completed
    7. While(True) for 5 and 6
    8. Terminate
    */

    }

    private int countLinesFile() throws FileNotFoundException {
        int count = 0;
        File file = new File(this.inputFilePath);
        Scanner sc = new Scanner(file);
        while(sc.hasNextLine()) {
            sc.nextLine();
            count++;
        }
        sc.close();
        return count;
    }
}
