package advancetest2.mapreduce.task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

class reducer extends Reducer<Text, Bean, Text, Bean> {
    private MultipleOutputs<Text, Bean> mos = null;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
        for (Bean i : values) {
            mos.write(key, i, key + "/");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        mos.close();
        mos = null;
    }
}
