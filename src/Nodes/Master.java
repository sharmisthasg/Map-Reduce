package Nodes;

import Config.MapReduceProperties;
import Constants.MRConstant;
import CustomException.MapReduceException;
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

    public void start() throws MapReduceException {
        try {
            System.out.println("Loading properties from config file....");
            MapReduceProperties mrProp = new MapReduceProperties();
            Properties prop = mrProp.getProperties();
            this.numOfWorkers = prop.getProperty("N");
            this.inputFilePath = prop.getProperty("input_file_path");
            this.outputFilePath = prop.getProperty("output_file_path");
            this.udfClass = prop.getProperty("udf_class");
            System.out.println("Properties Loaded => " + prop.values());
            execute();
        }catch(IOException e){
            throw new MapReduceException(e.getMessage());
        }
    }

    public void execute() throws MapReduceException{
        try {
            ServerSocket server = new ServerSocket(this.ioPort);
            System.out.println("Socket Server started");
            ActiveWorkers activeWorkers = ActiveWorkers.getInstance();
            List<String> reducerInputFiles = new ArrayList<>();
            System.out.printf("Running Mapper!");
            runMapper(activeWorkers, server, reducerInputFiles);
            System.out.println("Mapper Processes are Done!");
            System.out.println("Running Reducer!");
            runReducer(activeWorkers, server, reducerInputFiles);
        }catch(IOException e){
            throw new MapReduceException(e.getMessage());
        }
    }

    private void runMapper(ActiveWorkers activeWorkers, ServerSocket server, List<String> reducerInputFiles) throws MapReduceException {
        try {
            int numberOfLines = countLinesFile();
            int offset = (int) Math.ceil((double)numberOfLines/(double) Integer.parseInt(this.numOfWorkers));
            int startLine = 0;
            int workerId = 0;
            System.out.println("Spawning Mapper Processes");
            while (startLine < numberOfLines) {
                String commandList[] = {"java", "-cp", "out/production/MapReduceProject",
                        MRConstant.WORKER_JAVA_LOCATION,
                        String.valueOf(workerId),
                        String.valueOf(this.ioPort),
                        MRConstant.MAPPER,
                        this.inputFilePath,
                        this.udfClass,
                        String.valueOf(startLine),
                        String.valueOf(offset),
                        ""
                };
                // creating worker node process with workerType = Mapper
                ProcessBuilder processBuilder = new ProcessBuilder(commandList);
                processBuilder.inheritIO();
                Process process = processBuilder.start();
                activeWorkers.isActiveWorker.add(workerId);
                startLine += offset;
                workerId++;
            }
            System.out.println("Waiting for a client/mapper to connect to port...");
            Socket socket = server.accept();
            System.out.println("Client/Mapper accepted");

            // takes input from the client socket
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            String line = "";

            while (!activeWorkers.isActiveWorker.isEmpty()) {
                WorkerStatus status = (WorkerStatus) ois.readObject();
                System.out.println("Received From Mapper ==> " + status);
                if (MRConstant.SUCCESS.equals(status.getStatus())) {
                    activeWorkers.isActiveWorker.remove(status.getWorkerId());
                    reducerInputFiles.add(status.getFilePath());
                }
            }
            socket.close();
        }catch (Exception e){
            e.printStackTrace();
            throw new MapReduceException(e.getMessage());
        }
    }

    private void runReducer(ActiveWorkers activeWorkers, ServerSocket server, List<String> reducerInputFiles) throws MapReduceException {
        try {
            int workerId = 0;
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
                        "",
                        this.outputFilePath
                };
                // creating worker node process with workerType = Reducer
                ProcessBuilder processBuilder = new ProcessBuilder(commandList);
                processBuilder.inheritIO();
                Process process = processBuilder.start();
                activeWorkers.isActiveWorker.add(workerId);
                workerId++;
                reducers++;
            }
            System.out.println("Waiting for a client/reducer to connect to port...");
            Socket socket = server.accept();
            System.out.println("Client/Reducer accepted");

            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            String line = "";

            while (!activeWorkers.isActiveWorker.isEmpty()) {
                Object obj = ois.readObject();
                if(obj!=null) {
                    WorkerStatus status = (WorkerStatus) obj;
                    System.out.println("Received From Reducer ==> " + status);
                    if (MRConstant.SUCCESS.equals(status.getStatus())) {
                        activeWorkers.isActiveWorker.remove(status.getWorkerId());
                    }
                }
            }

        }catch(Exception e){
            e.printStackTrace();
            throw new MapReduceException(e.getMessage());
        }
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
