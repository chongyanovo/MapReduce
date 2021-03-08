package basictest2.task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

class reducer extends Reducer<Text, Bean, Text, IntWritable> {
    HashMap<String, Integer> map = new HashMap<String, Integer>();

    @Override
    protected void reduce(Text key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        HashSet set = new HashSet();
        for (Bean i : values) {
            if (!set.contains(i.getClazz())) {
                sum += i.getNum();
                set.add(i.getClazz());
            }
        }
        map.put(key.toString(), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (String i : map.keySet()) {
            sum += map.get(i);
        }
        context.write(new Text("人工智能学院的学生总数是:" + "\t"), new IntWritable(sum));
    }
}
