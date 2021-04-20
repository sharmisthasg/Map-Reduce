package Factory;

import Constants.MRConstant;
import Service.MRService;
import Service.Mapper;
import Service.Reducer;

import java.util.List;

public class WorkerFactory {

    public MRService getMapperReducerFactory(int workerId, String workerType, int ioPort, List<String> inputFilePath, String udfClass, String outputFilePath, String startLine, String offset, String numberOfWorkers, int nodeToCrash, boolean forceWorkerCrash, boolean forceWorkerException) {

        if(MRConstant.MAPPER.equals(workerType)){
            return new Mapper(workerId, workerType, ioPort, inputFilePath, udfClass, startLine, offset, numberOfWorkers, nodeToCrash, forceWorkerCrash, forceWorkerException);
        }else{
            return new Reducer(workerId, workerType, ioPort, inputFilePath, udfClass, outputFilePath, numberOfWorkers, nodeToCrash, forceWorkerCrash, forceWorkerException);
        }

    }
}
