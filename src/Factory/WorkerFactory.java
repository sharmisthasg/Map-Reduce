package Factory;

import Constants.MRConstant;
import Service.MRService;
import Service.Mapper;
import Service.Reducer;

import java.util.List;

public class WorkerFactory {

    public MRService getMapperReducerFactory(int workerId, String workerType, int ioPort, List<String> inputFilePath, String udfClass) {

        if(MRConstant.MAPPER.equals(workerType)){
            return new Mapper(workerId, workerType, ioPort, inputFilePath, udfClass);
        }else{
            return new Reducer(workerId, workerType, ioPort, inputFilePath, udfClass);
        }

    }
}
