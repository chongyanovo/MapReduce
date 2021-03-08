package basictest4.task2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

class reducer extends Reducer<Text, Bean, Bean, NullWritable> {
    //HashMap<DataBean22, Integer> map = new HashMap<DataBean22, Integer>();
    private TreeMap<Integer, Bean> mapMan = new TreeMap<Integer, Bean>();
    private TreeMap<Integer, Bean> mapWoman = new TreeMap<Integer, Bean>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(Text key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
        for (Bean i : values) {
            if (i.getSex().equals("男")) {
                mapMan.put(i.getAge(), i);
            } else {
                mapWoman.put(i.getAge(), i);
            }
            //context.write(i, NullWritable.get());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while (mapMan.size() > 1) {
            mapMan.remove(mapMan.firstKey());
        }
        while (mapWoman.size() > 1) {
            mapWoman.remove(mapWoman.firstKey());
        }
        context.write(mapMan.get(mapMan.firstKey()), NullWritable.get());
        context.write(mapWoman.get(mapWoman.firstKey()), NullWritable.get());
    }
}
