package ProcessStates;

import java.util.HashMap;
import java.util.Map;

public class ActiveWorkers {

    //TODO: Define Thread safe Singleton class to keep track of active workers

    private ActiveWorkers() {}
    private ActiveWorkers activeWorkers;
    private Map<String,Boolean> isActiveWorker;
    /*
    <workerid_1 : True>
    <workerid_1 : False>
     */

    public Map<String, Boolean> getIsActiveWorker() {
        return isActiveWorker;
    }

    public ActiveWorkers getInstance(){

        if(this.activeWorkers==null){
            this.activeWorkers = new ActiveWorkers();
            this.isActiveWorker = new HashMap<>();
        }
        return this.activeWorkers;
    }

}
