import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

    public  class ProbabilityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String carrier="";
            int arrival_time=0;
            int CRSArrival_time=0;
            int cancelled ;
            int possible_delay = 10;

            try {
                String[] rawData = value.toString().split(",");
                if (!rawData[0].equalsIgnoreCase("year")) {
                    cancelled = Integer.parseInt(rawData[21]);
                    if (cancelled != 1) {
                        arrival_time = Integer.parseInt(rawData[6]);
                        CRSArrival_time = Integer.parseInt(rawData[7]);
                        carrier = rawData[8];
                    }
                }

                if (arrival_time < CRSArrival_time || (arrival_time - CRSArrival_time) < possible_delay) {
                    context.write(new Text(carrier), new IntWritable(1));
                }

                context.write(new Text(carrier), new IntWritable(0));

            }catch (Exception e){
                System.out.println(e);
            }

        }
    }

