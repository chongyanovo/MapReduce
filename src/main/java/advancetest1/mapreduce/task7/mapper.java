package advancetest1.mapreduce.task7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

//7.计算每个城市的运营和非运营车辆的数量。
class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    HashMap<String, Integer> map = new HashMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(",");
        String nature = lines[10];//使用性质
        if (!nature.equals(null)) {
            if (map.containsKey(nature)) {
                int t = map.get(nature);
                t++;
                map.put(nature, t);
            } else {
                map.put(nature, 1);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String i : map.keySet()) {
            context.write(new Text(i), new IntWritable(map.get(i)));
        }
    }
}
