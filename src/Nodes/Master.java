package Nodes;

import Constants.MRConstant;

public class Master {


    private String configFile;

    public Master(String configFile){
        this.configFile=configFile;
    }
    public Master(){}

    public void execute(){
        /*TODO:
    1. Read from config file using Properties which creates a map of sorts
    2. Create N mapper (Worker class) processes and update ActiveWorkers
    3. Wait for N mapper process to get completed
    4. While(True) for 2 and 3
    5. On completion create N reducer (Worker class) processes and update ActiveWorkers
    6. Wait for N reducer process to get completed
    7. While(True) for 5 and 6
    8. Terminate
    */
    }
}
