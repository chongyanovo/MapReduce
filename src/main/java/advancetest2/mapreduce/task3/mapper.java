package advancetest2.mapreduce.task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;

//3、找出南京市1月份销售量最高的3个商品名称，格式如下
class mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    int K;
    TreeMap<Integer, Bean> map = new TreeMap<>();
    ArrayList<String> list = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        K = 3;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split(",");
        Bean myBean = new Bean();
        myBean.setShop(lines[1]);
        myBean.setGood(lines[2]);
        //int sum = Integer.parseInt(lines[3]) + Integer.parseInt(lines[5]);
        map.put(Integer.parseInt(lines[3]), myBean);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Integer i : map.keySet()) {
            list.add(map.get(i).toString() + "\t" + i);
        }
        Collections.reverse(list);
        for (int i = 0; i < K; i++) {
            context.write(new Text(list.get(i)), NullWritable.get());
        }
    }
}
