import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CleanMapper
    extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        // private Text line = new Text();

        @Override
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
                String line = value.toString();
                String[] array = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                ArrayList<String> columns = new ArrayList<>();
                if (array.length != 35) return;
                String id = array[0];
                columns.add(id);
                String borough = array[13];
                columns.add(borough);
                String crime_desc = array[10];
                columns.add(crime_desc);
                String latitude = array[27];
                columns.add(latitude);
                String longitude = array[28];
                columns.add(longitude);
                String violation = array[29];
                columns.add(violation);
                String newLine = "";
                for (String column : columns){
                    if (column.equals("")) return;
                    newLine = newLine + column + ",";
                }
                newLine = newLine.substring(0,newLine.length() - 1);
                context.write(new Text(newLine), new IntWritable(1));
        } 
}