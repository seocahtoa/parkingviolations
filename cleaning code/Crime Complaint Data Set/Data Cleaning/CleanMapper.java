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
                if (array.length != 20) return;
                String state = array[2];
                columns.add(state);
                String license_type = array[3];
                columns.add(license_type);
                String issue_date = array[5];
                columns.add(issue_date);
                String violation_time = array[7];
                columns.add(violation_time);
                String violation = array[8];
                columns.add(violation);
                String fine_amount = array[9];
                columns.add(fine_amount);
                String penalty_amount = array[10];
                columns.add(penalty_amount);
                String interest_amount = array[11];
                columns.add(interest_amount);
                String reduction_amount = array[12];
                columns.add(reduction_amount);
                String payment_amount = array[13];
                columns.add(payment_amount);
                String amount_due = array[14];
                columns.add(amount_due);
                String precinct = array[15];
                columns.add(precinct);
                String county = array[16];
                columns.add(county);
                String issuing_agency = array[17];
                columns.add(issuing_agency);
                String violation_status = array[18];
                columns.add(violation_status);
                String judgment_entry_date = array[19];
                columns.add(judgment_entry_date);
                String newLine = "";
                for (String column : columns){
                    if (column.equals("")) return;
                    newLine = newLine + column + ",";
                }
                newLine = newLine.substring(0,newLine.length() - 1);
                context.write(new Text(newLine), new IntWritable(1));
        } 
}