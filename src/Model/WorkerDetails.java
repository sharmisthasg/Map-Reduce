package Model;

public class WorkerDetails {
    private Process process;
    private String startLine;
    private String offset;
    private String inputFilePath;
    private String workerType;
    private String outputFilePath;

    public WorkerDetails() {
    }

    public WorkerDetails(Process process, String startLine, String offset, String inputFilePath, String workerType, String outputFilePath) {
        this.process = process;
        this.startLine = startLine;
        this.offset = offset;
        this.inputFilePath = inputFilePath;
        this.workerType = workerType;
        this.outputFilePath = outputFilePath;
    }

    public Process getProcess() {
        return process;
    }

    public void setProcess(Process process) {
        this.process = process;
    }

    public String getStartLine() {
        return startLine;
    }

    public void setStartLine(String startLine) {
        this.startLine = startLine;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public String getInputFilePath() {
        return inputFilePath;
    }

    public void setInputFilePath(String inputFilePath) {
        this.inputFilePath = inputFilePath;
    }

    public String getWorkerType() {
        return workerType;
    }

    public void setWorkerType(String workerType) {
        this.workerType = workerType;
    }

    public String getOutputFilePath() {
        return outputFilePath;
    }

    public void setOutputFilePath(String outputFilePath) {
        this.outputFilePath = outputFilePath;
    }

    @Override
    public String toString() {
        return "WorkerDetails{" +
                "process=" + process +
                ", startLine=" + startLine +
                ", offset=" + offset +
                ", inputFilePath='" + inputFilePath + '\'' +
                ", workerType='" + workerType + '\'' +
                ", outputFilePath='" + outputFilePath + '\'' +
                '}';
    }
}
