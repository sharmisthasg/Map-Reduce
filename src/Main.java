import Nodes.Master;

public class Main {

    public static void main(String[] args) {
        //Kickstart Master
        System.out.println("Starting MapReduce..");
        Master master = new Master("");
        master.execute();
    }

}
