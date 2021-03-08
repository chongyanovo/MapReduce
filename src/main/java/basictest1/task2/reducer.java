package basictest1.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

class reducer extends Reducer<Text, Text, Text, IntWritable> {
    Set set = new HashSet<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (Text i : values) {
            if (!set.contains(i)) {
                set.add(i);
                count++;
            }
        }
        context.write(key, new IntWritable(count));
    }
}
