package Service;

import java.util.List;

public class Reducer implements MRService{

    private int id;
    private int ioPort;
    private String workerType;
    private List<String> inputFilePath;
    private String udfClass;

    public Reducer(int id, String workerType, int ioPort, List<String> inputFilePath, String udfClass) {
        this.id = id;
        this.ioPort = ioPort;
        this.workerType = workerType;
        this.inputFilePath = inputFilePath;
        this.udfClass = udfClass;
    }

    @Override
    public void execute() {

    }

}
