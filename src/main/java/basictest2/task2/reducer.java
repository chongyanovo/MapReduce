package basictest2.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

class reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    TreeMap<Integer, String> map = new TreeMap<Integer, String>();
    int K = 1;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        int sum = 0;
        for (IntWritable i : values) {
            count++;
            sum += i.get();
        }
        map.put(sum / count, key.toString());
        if (map.size() > K) {
            map.remove(map.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Object i : map.keySet()) {
            context.write(new Text(map.get(i)), new IntWritable((int) i));
        }
    }
}
