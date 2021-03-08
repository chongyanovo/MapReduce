package basictest1.task1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Double sum = 0.0;
        int count = 0;
        for (DoubleWritable i : values) {
            sum += i.get();
            count++;
        }
        Double result = sum / count;
        context.write(key, new DoubleWritable(result));
    }
}
