import java.util.HashMap;
import java.util.Vector;

import static java.lang.Math.abs;

public class ReadMapShuffleMultithread extends Thread implements Runnable {
    private int actual_thread;
    private String absolutePath;
    private int num_reducers;
    private HashMap<Integer, Map> sharedMaps;
    private HashMap<Integer, Vector<Reduce>> sharedReducers;
    
    public ReadMapShuffleMultithread(int actual_thread, String absolutePath, int num_reducers, HashMap<Integer, Map> sharedMaps, HashMap<Integer, Vector<Reduce>> sharedReducers) {
        this.actual_thread = actual_thread;
        this.absolutePath = absolutePath;
        this.num_reducers = num_reducers;
        this.sharedMaps = sharedMaps;
        this.sharedReducers = sharedReducers;
    }

    @Override
    public void run() {
        //READ THE FILE and SET THE MAP
        System.out.println("Thread number " + actual_thread + " processing input file " + absolutePath + "\n");
        if(sharedMaps.get(actual_thread).ReadFileTuples(absolutePath)!=Error.COk)
        {
            Error.showError("MapReduce::Split Read error.\n");
        }
        //RUN MAP
        System.out.println("Thread number " + actual_thread + " executing mapping phase");
        if (sharedMaps.get(actual_thread).Run()!=Error.COk)
        {
            Error.showError("MapReduce::Map Run error.\n");
        }
        //RUN SHUFFLE
        System.out.println("Thread number " + actual_thread + " executing shuffle phase");
        if (MapReduce.DEBUG) sharedMaps.get(actual_thread).PrintOutputs();
        for (String key : sharedMaps.get(actual_thread).GetOutput().keySet())
        {
            int r = abs(key.hashCode()%num_reducers);
            if (MapReduce.DEBUG) System.err.println("DEBUG::MapReduce::Suffle merge key " + key +" to reduce " + r);
            sharedReducers.get(actual_thread).get(r).AddInputKeys(key, sharedMaps.get(actual_thread).GetOutput().get(key));
        }
        sharedMaps.get(actual_thread).GetOutput().clear();
    }
}
