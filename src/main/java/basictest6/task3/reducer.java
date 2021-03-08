package basictest6.task3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;

class reducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
    TreeMap<Double, String> map = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;
        for (DoubleWritable i : values) {
            sum += i.get();
            count++;
        }
        map.put(sum / count, key.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        ArrayList<String> list = new ArrayList();
        for (Double i : map.keySet()) {
            list.add(map.get(i) + "\t" + i);
        }
        Collections.reverse(list);
        for (String j : list) {
            context.write(new Text(j), NullWritable.get());
        }
    }
}
