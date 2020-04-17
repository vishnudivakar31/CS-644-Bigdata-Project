package edu.njit.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Top3Airports {

    private static final Integer ORIGIN_INDEX = 16, DEST_INDEX = 17, TAXI_OUT_INDEX = 20, TAXI_IN_INDEX = 19;

    public static class AirportMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    public static class AirportReducer extends Reducer<Text, IntWritable, Text, Text> {
        private Map<String, Double> resultMap;

        @Override
        protected void setup(Context context) {
            resultMap = new HashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int count = 0, sum = 0;
            for(IntWritable value : values) {
                sum += value.get();
                count++;
            }
            Double avgTaxiTime = (double) (sum) / (double) (count);
            resultMap.put(key.toString(), avgTaxiTime);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<String, Double> shortestTaxi;
            Map<String, Double> longestTaxi;
            DecimalFormat df = new DecimalFormat("0.0000");

            shortestTaxi = resultMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (v1, v2) -> v1, LinkedHashMap::new));

            longestTaxi = resultMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                            (v1, v2) -> v1, LinkedHashMap::new));

            int counter = 0;
            context.write(new Text("Total airport entries"), new Text(String.valueOf(longestTaxi.entrySet().size())));
            context.write(new Text("Airports with Longest Average Taxi Time"), new Text("Average Time in Minutes"));
            for(Map.Entry<String, Double> entry : longestTaxi.entrySet()) {
                String value = df.format(entry.getValue());
                context.write(new Text(entry.getKey()), new Text(value));
                counter++;
                if(counter == 3) break;
            }

            counter = 0;
            context.write(new Text("Airports with Shortest Average Taxi Time"), new Text("Average Time in Minutes"));
            for(Map.Entry<String, Double> entry : shortestTaxi.entrySet()) {
                String value = df.format(entry.getValue());
                context.write(new Text(entry.getKey()), new Text(value));
                counter++;
                if(counter == 3) break;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "top 3 airports");
        job.setJarByClass(Top3Airports.class);

        job.setMapperClass(AirportMapper.class);
        job.setReducerClass(AirportReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
