package ProcessStates;

import Model.WorkerDetails;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ActiveWorkers {

    private ActiveWorkers() {
        this.isActiveWorker = new HashMap<>();
    }
    private static ActiveWorkers activeWorkers = null;
    public Map<Integer, WorkerDetails> isActiveWorker;
    public static ActiveWorkers getInstance(){
        if(activeWorkers==null){
            activeWorkers = new ActiveWorkers();
        }
        return activeWorkers;
    }

}
