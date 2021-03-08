package advancetest1.mapreduce.task3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;

class reducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    HashMap<String, Integer> map = new HashMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable i : values) {
            if (!key.toString().equals(null)) {
                count += i.get();
            }
        }
        map.put(key.toString(), count);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (String i : map.keySet()) {
            sum += map.get(i);
        }
        for (String j : map.keySet()) {
            DecimalFormat decimalFormat = new DecimalFormat("##.00");
            Double str = Double.parseDouble(decimalFormat.format(map.get(j) / sum));
            context.write(new Text(j), new DoubleWritable(str));
        }
    }
}
