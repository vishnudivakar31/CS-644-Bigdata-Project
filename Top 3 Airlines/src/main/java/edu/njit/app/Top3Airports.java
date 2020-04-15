package edu.njit.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class Top3Airports {

    private static final Integer ORIGIN_INDEX = 16, DEST_INDEX = 17, TAXI_IN_INDEX = 19, TAXI_OUT_INDEX = 20;
    private static Map<String, String> airportCodeMap = new HashMap<>();

    public static class AirportMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] rawData = value.toString().split(",");
            try {
                if(rawData.length == 29) {
                    Integer taxiInTime = Integer.parseInt(rawData[TAXI_IN_INDEX]);
                    Integer taxiOutTime = Integer.parseInt(rawData[TAXI_OUT_INDEX]);
                    String originCode = rawData[ORIGIN_INDEX];
                    String destCode = rawData[DEST_INDEX];
                    context.write(new Text(originCode), new Text(String.format("%d,1", taxiOutTime)));
                    context.write(new Text(destCode), new Text(String.format("%d,1", taxiInTime)));
                }
            } catch (NumberFormatException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public static class AirportReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        Map<Double, Text> result;

        @Override
        protected void setup(Context context) {
            result = new HashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
            int totalCount = 0, totalTaxiTime = 0;
            for(Text value : values) {
                String[] data = value.toString().split(",");
                int taxiTime = Integer.parseInt(data[0]);
                int count = Integer.parseInt(data[1]);
                totalTaxiTime += taxiTime;
                totalCount += count;
            }
            result.put(((double) totalTaxiTime / (double) totalCount), key);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Double, Text> airportByShortestTaxiTime = new HashMap<>();
            Map<Double, Text> airportByLongestTaxiTime = new HashMap<>();

            result.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEachOrdered(item -> airportByShortestTaxiTime.put(item.getKey(), item.getValue()));

            result.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey(Comparator.reverseOrder()))
                    .forEachOrdered(item -> airportByLongestTaxiTime.put(item.getKey(), item.getValue()));

            int counter = 0;
            for(Map.Entry<Double, Text> airport : airportByLongestTaxiTime.entrySet()) {
                String key = airport.getValue().toString();
                context.write(new Text(airportCodeMap.get(key)), new DoubleWritable(airport.getKey()));
                counter++;
                if(counter == 3) break;
            }
            counter = 0;
            for(Map.Entry<Double, Text> airport : airportByShortestTaxiTime.entrySet()) {
                String key = airport.getValue().toString();
                context.write(new Text(airportCodeMap.get(key)), new DoubleWritable(airport.getKey()));
                counter++;
                if(counter == 3) break;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        if(args.length < 3) {
            System.out.println("Please provide an input file, an output directory and airport code file");
            System.exit(-1);
        }

        loadAirportCodeMap(args[2], configuration);

        Job job = Job.getInstance(configuration, "Top 3 Airports");
        job.setJarByClass(Top3Airports.class);

        job.setMapperClass(AirportMapper.class);
        job.setReducerClass(AirportReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void loadAirportCodeMap(String airportFilePath, Configuration configuration) throws IOException {
        String line;
        Path path = new Path(airportFilePath);
        FileSystem fileSystem = FileSystem.get(configuration);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        while((line = bufferedReader.readLine())!= null) {
            String[] data = line.split(",");
            String airportCode = data[0];
            String airportName = data[1];
            String city = data[2];
            String state = data[3];
            String country = data[4];
            airportCodeMap.put(airportCode, String.format("%s- %s, %s, %s", airportName, city, state, country));
        }
    }
}
