import Nodes.Master;

public class Main {

    public static void main(String[] args) {
        //Kickstart Master
        System.out.println("Starting MapReduce..");
        Master master = new Master();
        try {
            master.start();
        }catch (Exception e){
            System.out.println("Error: " + e.getMessage());
            System.out.println("StackTrace: " + e.getStackTrace());
        }
    }

}
