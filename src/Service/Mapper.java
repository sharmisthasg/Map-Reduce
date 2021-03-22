package Service;

import Constants.MRConstant;
import DataType.KeyValuePair;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

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
    public void execute() {
        /*
        1. load file using inputFilePath
        2. docId, String {entire document)
        3. Using java reflection call
         */

    }

    public void write()
    {
        try {
            String filename = "intermediate-mapper-"+String.valueOf(startLine)+"-"+String.valueOf(offset)+".txt";
            FileWriter fileWriter = new FileWriter("intermediate/"+filename);

            for(KeyValuePair kp: kvpairs)
            {
                fileWriter.write(kp.toKeyValueString());
            }

            fileWriter.close();
        }catch (Exception e){
            System.out.println(MRConstant.FILE_WRITE_EXCEPTION);
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
