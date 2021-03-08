package basictest4.task1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class reducer extends Reducer<Bean, NullWritable, Bean, NullWritable> {
    @Override
    protected void reduce(Bean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
