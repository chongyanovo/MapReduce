package basictest1.task3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

class reducer extends Reducer<Bean, Text, Text, Text> {
    /*
        int K = 1;
    int count = 0;
    @Override
    protected void reduce(Bean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text i : values) {
            if (count < K) {
               context.write(new Text(key.getLocation()),i);
            }
            count++;
        }
    }
     */

    int K = 1;
    TreeMap<Integer, String> map = new TreeMap<Integer, String>();

    @Override
    protected void reduce(Bean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text i : values) {
            map.put(key.getHot(), key.getLocation() + "\t" + i.toString());
            if (map.size() > K) {
                map.remove(map.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Integer i : map.keySet()) {
            context.write(new Text(map.get(i)), new Text(""));
        }
    }
}
