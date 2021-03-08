package advancetest2.mapreduce.task5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;

class reducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    int K;
    TreeMap<Integer, String> map = new TreeMap<>();
    ArrayList<String> list = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        K = 1;
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        map.put(sum, key.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Integer i : map.keySet()) {
            list.add(map.get(i));
        }
        Collections.reverse(list);
        for (int i = 0; i < K; i++) {
            context.write(new Text(list.get(i)), NullWritable.get());
        }
    }
}
