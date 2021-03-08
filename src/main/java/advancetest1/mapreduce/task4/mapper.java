package advancetest1.mapreduce.task4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;

//4、找出2013年山西销售量最高的前3位的汽车品牌
class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    TreeMap<String, Integer> map = new TreeMap<>();
    int K = 3;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(",");
        String brand = lines[7];
        if (map.containsKey(brand)) {
            int t = 1;
            t += map.get(brand);
            map.put(brand, t);
        } else {
            map.put(brand, 1);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        ArrayList<String> list = new ArrayList<>();
        for (String i : map.keySet()) {
            list.add(i);
        }
        Collections.reverse(list);
        for (int i = 0; i < K; i++) {
            String str = list.get(i);
            context.write(new Text(str), new IntWritable(map.get(str)));
        }
    }
}
