import java.util.HashMap;
import java.util.Vector;

public class ReduceMultithread extends Thread implements Runnable {
    private int actual_thread;
    private HashMap<Integer, Vector<Reduce>> sharedReducersPerFile;

    public ReduceMultithread(int actual_thread, HashMap<Integer, Vector<Reduce>> sharedReducersPerFile) {
        this.actual_thread = actual_thread;
        this.sharedReducersPerFile = sharedReducersPerFile;
    }

    @Override
    public void run() {
        System.out.println("Thread number " + actual_thread + " executing reduce phase");
        for(int i=0; i<sharedReducersPerFile.get(actual_thread).size(); ++i)
        {
            if(sharedReducersPerFile.get(actual_thread).get(i).Run() != Error.COk)
            {
                Error.showError("MapReduce::Reduce Run error.\n");
            }
        }
    }
}
