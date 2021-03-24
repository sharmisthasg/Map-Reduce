package ProcessStates;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ActiveWorkers {

    //TODO: Define Thread safe Singleton class to keep track of active workers

    private ActiveWorkers() {
        this.isActiveWorker = new HashSet<>();
    }
    private static ActiveWorkers activeWorkers = null;
    public Set<Integer> isActiveWorker;
    /*
    <workerid_1 : True>
    <workerid_1 : False>
     */

    public static ActiveWorkers getInstance(){

        if(activeWorkers==null){
            activeWorkers = new ActiveWorkers();
        }
        return activeWorkers;
    }

}
