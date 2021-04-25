package Nodes;

import Config.MapReduceProperties;
import Constants.MRConstant;
import CustomException.MapReduceException;
import Model.WorkerDetails;
import Model.WorkerStatus;
import ProcessStates.ActiveWorkers;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketOption;
import java.util.*;
import java.util.stream.Collectors;

public class Master {

    String numOfWorkers;
    String inputFilePath;
    String outputFilePath;
    String udfClass;
    int ioPort = 5001;
    boolean forceWorkerException = false;
    boolean forceWorkerCrash = false;
    int nodeToCrash = 0;

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
            this.forceWorkerCrash = MRConstant.TRUE.equals(prop.getProperty("crash_worker"));
            this.forceWorkerException = MRConstant.TRUE.equals(prop.getProperty("exception_worker"));
            if(this.forceWorkerCrash && this.forceWorkerException){
                System.out.println("Both Cannot be true, Will be forcing only Worker Crash!");
                this.forceWorkerException=false;
            }
            System.out.println("Properties Loaded => " + prop.values());
            setup(this.udfClass,this.outputFilePath);
            cleanUp(this.udfClass,this.outputFilePath);
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
            Map<String,List<String>> reducerInputFiles = new HashMap<>();
            System.out.println("Running Mapper!");
            runMapper(activeWorkers, server, reducerInputFiles);
            System.out.println("Mapper Processes are Done!");
            System.out.println("Running Reducer!");
            //System.out.println(reducerInputFiles);
            runReducer(activeWorkers, server, reducerInputFiles);
        }catch(IOException e){
            throw new MapReduceException(e.getMessage());
        }
    }

    private void runMapper(ActiveWorkers activeWorkers, ServerSocket server, Map<String,List<String>> reducerInputFiles) throws MapReduceException {
        try {
            //counting number of lines in input File and splitting lines between number of workers
            int numberOfLines = countLinesFile();
            // Calculating the offset i.e. the max number of lines that each worker should read which is equal to rounding down to the nearest integer of (number of lines)/(number of Workers)
            int offset = (int) Math.ceil((double)numberOfLines/(double) Integer.parseInt(this.numOfWorkers));
            // When the particular Worker should start reading the file
            int startLine = 0;
            int workerId = 0;
            System.out.println("Spawning Mapper Processes");
            WorkerDetails workerDetails = null;
            while (startLine < numberOfLines) {
                //Last worker should read all the files so increasing the offset value
                if(workerId+1 == Integer.parseInt(this.numOfWorkers)){
                    offset += numberOfLines % Integer.parseInt(this.numOfWorkers);
                }
                String commandList[] = {"java", "-cp", "src/",
                        MRConstant.WORKER_JAVA_LOCATION,
                        String.valueOf(workerId),
                        String.valueOf(this.ioPort),
                        MRConstant.MAPPER,
                        this.inputFilePath,
                        this.udfClass,
                        String.valueOf(startLine),
                        String.valueOf(offset),
                        "",
                        this.numOfWorkers,
                        String.valueOf(this.nodeToCrash),
                        String.valueOf(this.forceWorkerCrash),
                        String.valueOf(this.forceWorkerException)
                };
                // creating worker node process with workerType = Mapper
                ProcessBuilder processBuilder = new ProcessBuilder(commandList);
                processBuilder.inheritIO();
                //starting process
                Process process = processBuilder.start();
                workerDetails = new WorkerDetails(process,String.valueOf(startLine),String.valueOf(offset),this.inputFilePath,MRConstant.MAPPER,"");
                activeWorkers.isActiveWorker.put(workerId,workerDetails);
                startLine += offset;
                workerId++;
            }
            ObjectInputStream ois = null;
            String line = "";
            Map<String,String> outputFileMap = null;
            Socket socket=null;
            Map<Integer, WorkerDetails> failedWorker = new HashMap<>();
            boolean workerFailed = false;
            while (!activeWorkers.isActiveWorker.isEmpty()) {
                if((this.forceWorkerException || this.forceWorkerCrash) && activeWorkers.isActiveWorker.keySet().size() == 1){
                    int wid=0;
                    WorkerDetails workerDesc = null;
                    for(int key : activeWorkers.isActiveWorker.keySet()) {
                        wid = key;
                        workerDesc = activeWorkers.isActiveWorker.get(key);
                    }
                    if(!workerDesc.getProcess().isAlive()){
                        System.out.println("Worker ID: " + wid + " has been terminated prematurely!!!! Respawning " + workerDesc.getWorkerType() + " with Worker ID="+wid);
                        failedWorker.put(wid,workerDesc);
                        workerFailed=true;
                        activeWorkers.isActiveWorker.remove(wid);
                        break;
                    }
                }
                // takes input from the client socket
                try {
                    socket = server.accept();
                    ois = new ObjectInputStream(socket.getInputStream());
                }catch(EOFException e){
                    //Exception thrown because one of the worker processes crashed
                    continue;
                }
                WorkerStatus status = (WorkerStatus) ois.readObject();
                System.out.println("Received From Mapper ==> " +     status);
                if (MRConstant.SUCCESS.equals(status.getStatus())) {
                    activeWorkers.isActiveWorker.remove(status.getWorkerId());
                    //Getting intermediate File Path from Mapper which is a map containing the reducer_id that the file is expected to read from
                    outputFileMap = status.getFilePath();
                    for (Map.Entry<String, String> outputFileEntry : outputFileMap.entrySet()) {
                        if (!reducerInputFiles.containsKey(outputFileEntry.getKey())) {
                            reducerInputFiles.put(outputFileEntry.getKey(), new ArrayList<>());
                        }
                        reducerInputFiles.get(outputFileEntry.getKey()).add(outputFileEntry.getValue());
                    }
                }
            }
            while(workerFailed){
               workerFailed = respawnMapperProcess(failedWorker, activeWorkers, reducerInputFiles, server);
            }
            socket.close();
        }catch (Exception e){
            e.printStackTrace();
            throw new MapReduceException(e.getMessage());
        }
    }

    private boolean respawnMapperProcess(Map<Integer, WorkerDetails> failedWorker, ActiveWorkers activeWorkers,
                                         Map<String, List<String>> reducerInputFiles, ServerSocket server) throws IOException, ClassNotFoundException {
        System.out.println("ReSpawning Process");
        Map.Entry<Integer,WorkerDetails> entry = failedWorker.entrySet().iterator().next();
        int workerId = entry.getKey();
        WorkerDetails oldWorkerDetails = entry.getValue();
        failedWorker.remove(workerId);
        WorkerDetails newWorkerDetails = null;
        String commandList[] = {"java", "-cp", "src/",
                MRConstant.WORKER_JAVA_LOCATION,
                String.valueOf(workerId),
                String.valueOf(this.ioPort),
                oldWorkerDetails.getWorkerType(),
                oldWorkerDetails.getInputFilePath(),
                this.udfClass,
                String.valueOf(oldWorkerDetails.getStartLine()),
                String.valueOf(oldWorkerDetails.getOffset()),
                oldWorkerDetails.getOutputFilePath(),
                this.numOfWorkers,
                String.valueOf(this.nodeToCrash),
                "false",
                "false"
        };
        // creating worker node process
        ProcessBuilder processBuilder = new ProcessBuilder(commandList);
        processBuilder.inheritIO();
        //starting process
        Process process = processBuilder.start();
        newWorkerDetails = new WorkerDetails(process,String.valueOf(oldWorkerDetails.getStartLine()),
                String.valueOf(oldWorkerDetails.getOffset()),oldWorkerDetails.getInputFilePath(),oldWorkerDetails.getWorkerType(),
                oldWorkerDetails.getOutputFilePath());
        activeWorkers.isActiveWorker.put(workerId,newWorkerDetails);
        ObjectInputStream ois = null;
        String line = "";
        Map<String,String> outputFileMap = null;
        Socket socket=null;
        boolean workerFailed = false;
        while (!activeWorkers.isActiveWorker.isEmpty()) {
            if((this.forceWorkerException || this.forceWorkerCrash) && activeWorkers.isActiveWorker.keySet().size() == 1){
                int wid=0;
                WorkerDetails workerDesc = null;
                for(int key : activeWorkers.isActiveWorker.keySet()) {
                    wid = key;
                    workerDesc = activeWorkers.isActiveWorker.get(key);
                }
                if(!workerDesc.getProcess().isAlive()){
                    System.out.println("Worker ID: " + wid + " has been terminated prematurely!!!! Respawning " + workerDesc.getWorkerType() + " with Worker ID="+wid);
                    failedWorker.put(wid,workerDesc);
                    workerFailed=true;
                    activeWorkers.isActiveWorker.remove(wid);
                    break;
                }
            }
            // takes input from the client socket
            try {
                socket = server.accept();
                ois = new ObjectInputStream(socket.getInputStream());
            }catch(EOFException e){
                //Exception thrown because one of the worker processes crashed
                continue;
            }
            WorkerStatus status = (WorkerStatus) ois.readObject();
            System.out.println("Received From Mapper ==> " + status);
            if (MRConstant.SUCCESS.equals(status.getStatus())) {
                activeWorkers.isActiveWorker.remove(status.getWorkerId());
                //Getting intermediate File Path from Mapper which is a map containing the reducer_id that the file is expected to read from
                outputFileMap = status.getFilePath();
                for (Map.Entry<String, String> outputFileEntry : outputFileMap.entrySet()) {
                    if (!reducerInputFiles.containsKey(outputFileEntry.getKey())) {
                        reducerInputFiles.put(outputFileEntry.getKey(), new ArrayList<>());
                    }
                    reducerInputFiles.get(outputFileEntry.getKey()).add(outputFileEntry.getValue());
                }
            }
        }
        if(workerFailed){
            socket.close();
            return true;
        }
        socket.close();
        return false;
    }

    private void runReducer(ActiveWorkers activeWorkers, ServerSocket server, Map<String,List<String>> reducerInputFiles) throws MapReduceException {
        try {
            int workerId = 0;
            String reducerFilesStr = null;
            int reducers = 0;
            WorkerDetails workerDetails = null;
            while (reducers < Integer.parseInt(this.numOfWorkers)) {
                /*Sending the location of the files that a particular reducer should read.
                As the reducerInputFiles map data structure contains the reducer_id as the key which will always be between 0 and N-1,
                we can use the getOrDefault() function to get the corresponding intermediate files for the reducer. The hash function in the Mapper class
                makes sure that each key is matched to exactly one Reducer
                */
                reducerFilesStr = String.join(",",reducerInputFiles.getOrDefault(String.valueOf(workerId),new ArrayList<>()));
                //System.out.println("reducerFilesStr===>"+reducerFilesStr);
                String commandList[] = {"java", "-cp", "src/",
                        MRConstant.WORKER_JAVA_LOCATION,
                        String.valueOf(workerId),
                        String.valueOf(this.ioPort),
                        MRConstant.REDUCER,
                        reducerFilesStr,
                        this.udfClass,
                        "",
                        "",
                        this.outputFilePath,
                        this.numOfWorkers,
                        String.valueOf(this.nodeToCrash),
                        String.valueOf(this.forceWorkerCrash),
                        String.valueOf(this.forceWorkerException)
                };
                // creating worker node process with workerType = Reducer
                ProcessBuilder processBuilder = new ProcessBuilder(commandList);
                processBuilder.inheritIO();
                Process process = processBuilder.start();
                workerDetails = new WorkerDetails(process,"","",reducerFilesStr,MRConstant.REDUCER,this.outputFilePath);
                activeWorkers.isActiveWorker.put(workerId,workerDetails);
                workerId++;
                reducers++;
            }
            ObjectInputStream ois = null;
            Socket socket = null;
            Map<Integer, WorkerDetails> failedWorker = new HashMap<>();
            boolean workerFailed = false;
            while (!activeWorkers.isActiveWorker.isEmpty()) {
                if((this.forceWorkerException || this.forceWorkerCrash) && activeWorkers.isActiveWorker.keySet().size() == 1){
                    WorkerDetails workerDesc = null;
                    int wid =0;
                    for(int key : activeWorkers.isActiveWorker.keySet()) {
                        wid = key;
                        workerDesc = activeWorkers.isActiveWorker.get(key);
                    }
                    if(!workerDesc.getProcess().isAlive()){
                        System.out.println("Worker ID: " + wid + " has been terminated prematurely!!!! Respawning " + workerDesc.getWorkerType() + " with Worker ID="+wid);
                        failedWorker.put(wid,workerDesc);
                        workerFailed=true;
                        activeWorkers.isActiveWorker.remove(wid);
                        break;
                    }
                }
                // takes input from the client socket which is the reducer node in this case
                try {
                    socket = server.accept();
                    ois = new ObjectInputStream(socket.getInputStream());
                }catch(EOFException e){
                    //Exception thrown because one of the worker processes crashed
                    continue;
                }
                Object obj = ois.readObject();
                if(obj!=null) {
                    WorkerStatus status = (WorkerStatus) obj;
                    System.out.println("Received From Reducer ==> " + status);
                    if (MRConstant.SUCCESS.equals(status.getStatus())) {
                        activeWorkers.isActiveWorker.remove(status.getWorkerId());
                    }
                }
            }
            while(workerFailed){
                workerFailed = respawnReducerProcess(failedWorker, activeWorkers, server);
            }
            socket.close();
        }catch(Exception e){
            e.printStackTrace();
            throw new MapReduceException(e.getMessage());
        }
    }

    private boolean respawnReducerProcess(Map<Integer, WorkerDetails> failedWorker, ActiveWorkers activeWorkers, ServerSocket server) throws IOException, ClassNotFoundException {
        System.out.println("ReSpawning Process");
        Map.Entry<Integer,WorkerDetails> entry = failedWorker.entrySet().iterator().next();
        int workerId = entry.getKey();
        WorkerDetails oldWorkerDetails = entry.getValue();
        failedWorker.remove(workerId);
        WorkerDetails newWorkerDetails = null;
        String commandList[] = {"java", "-cp", "src/",
                MRConstant.WORKER_JAVA_LOCATION,
                String.valueOf(workerId),
                String.valueOf(this.ioPort),
                oldWorkerDetails.getWorkerType(),
                oldWorkerDetails.getInputFilePath(),
                this.udfClass,
                String.valueOf(oldWorkerDetails.getStartLine()),
                String.valueOf(oldWorkerDetails.getOffset()),
                oldWorkerDetails.getOutputFilePath(),
                this.numOfWorkers,
                String.valueOf(this.nodeToCrash),
                "false",
                "false"
        };
        // creating worker node process
        ProcessBuilder processBuilder = new ProcessBuilder(commandList);
        processBuilder.inheritIO();
        //starting process
        Process process = processBuilder.start();
        newWorkerDetails = new WorkerDetails(process,String.valueOf(oldWorkerDetails.getStartLine()),
                String.valueOf(oldWorkerDetails.getOffset()),oldWorkerDetails.getInputFilePath(),oldWorkerDetails.getWorkerType(),
                oldWorkerDetails.getOutputFilePath());
        activeWorkers.isActiveWorker.put(workerId,newWorkerDetails);
        ObjectInputStream ois = null;
        String line = "";
        Map<String,String> outputFileMap = null;
        Socket socket=null;
        boolean workerFailed = false;
        while (!activeWorkers.isActiveWorker.isEmpty()) {
            if((this.forceWorkerException || this.forceWorkerCrash) && activeWorkers.isActiveWorker.keySet().size() == 1){
                WorkerDetails workerDesc = null;
                int wid =0;
                for(int key : activeWorkers.isActiveWorker.keySet()) {
                    wid = key;
                    workerDesc = activeWorkers.isActiveWorker.get(key);
                }
                if(!workerDesc.getProcess().isAlive()){
                    System.out.println("Worker ID: " + wid + " has been terminated prematurely!!!! Respawning " + workerDesc.getWorkerType() + " with Worker ID="+wid);
                    failedWorker.put(wid,workerDesc);
                    workerFailed=true;
                    activeWorkers.isActiveWorker.remove(wid);
                    break;
                }
            }
            // takes input from the client socket
            try {
                socket = server.accept();
                ois = new ObjectInputStream(socket.getInputStream());
            }catch(EOFException e){
                //Exception thrown because one of the worker processes crashed
                continue;
            }
            Object obj = ois.readObject();
            if(obj!=null) {
                WorkerStatus status = (WorkerStatus) obj;
                System.out.println("Received From Reducer ==> " + status);
                if (MRConstant.SUCCESS.equals(status.getStatus())) {
                    activeWorkers.isActiveWorker.remove(status.getWorkerId());
                }
            }
        }
        if(workerFailed){
            socket.close();
            return true;
        }
        socket.close();
        return false;
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

    private void setup(String udfClass, String outputFilePath) {
        if(this.forceWorkerException || this.forceWorkerCrash){
            this.nodeToCrash = randomNodeIdGenerator();
            System.out.println("Worker ID to crash ==> " + this.nodeToCrash);
        }
        createOutputFolder(udfClass,outputFilePath);
        createIntermediateFolder(udfClass,outputFilePath);
    }

    private int randomNodeIdGenerator() {
        Random rand = new Random();
        return rand.nextInt(Integer.parseInt(this.numOfWorkers));
    }

    private void createOutputFolder(String udfClass, String outputFilePath) {
        File newDir = new File(outputFilePath+"/"+udfClass);
        if(!newDir.exists()){
            newDir.mkdirs();
        }
    }

    private void createIntermediateFolder(String udfClass, String outputFilePath) {
        File newDir = new File(MRConstant.MAP_OUTPUT_DIR + udfClass);
        if(!newDir.exists()){
            newDir.mkdirs();
        }
    }

    private void cleanUp(String udfClass, String outputFilePath) {
        deleteIntermediateFiles(udfClass,outputFilePath);
        deleteOutputFiles(udfClass,outputFilePath);
    }

    private void deleteOutputFiles(String udfClass, String outputFilePath) {
        File dir = new File(outputFilePath+"/"+udfClass);
        File[] listOfFiles = dir.listFiles();
        if(listOfFiles==null || listOfFiles.length==0){
            return;
        }
        for (File file: listOfFiles) {
            if (!file.isDirectory()) {
                file.delete();
            }
        }
    }

    private void deleteIntermediateFiles(String udfClass, String outputFilePath) {
        File dir = new File(MRConstant.MAP_OUTPUT_DIR + udfClass);
        File[] listOfFiles = dir.listFiles();
        if(listOfFiles==null || listOfFiles.length==0){
            return;
        }
        for (File file: listOfFiles) {
            if (!file.isDirectory()) {
                file.delete();
            }
        }
    }



}
