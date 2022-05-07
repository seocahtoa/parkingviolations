import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class CleanReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private Text line = new Text();
    public void reduce(Text key, Iterable<Text> values,Context context) 
        throws IOException, InterruptedException {
            // String table = "";
            // for (Text val : values) {
            //     table += val.toString() + "\n";
            // }
            context.write(key, null);
    }
}