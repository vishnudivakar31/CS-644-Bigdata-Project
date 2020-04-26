import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportTaxiMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Integer ORIGIN_INDEX = 16, DEST_INDEX = 17, TAXI_OUT_INDEX = 20, TAXI_IN_INDEX = 19;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] rawData = value.toString().split(",");
        if(!rawData[0].equalsIgnoreCase("year")) {
            String origin = rawData[ORIGIN_INDEX].trim();
            String dest = rawData[DEST_INDEX].trim();
            try {
                int taxiOutTime = Integer.parseInt(rawData[TAXI_OUT_INDEX]);
                int taxiInTime = Integer.parseInt(rawData[TAXI_IN_INDEX]);
                context.write(new Text(origin), new IntWritable(taxiOutTime));
                context.write(new Text(dest), new IntWritable(taxiInTime));
            } catch (NumberFormatException e) {
                
            }
        }
    }
}