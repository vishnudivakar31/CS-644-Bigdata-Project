package edu.njit.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Top3Airports {

    private static final Integer ORIGIN_INDEX = 16, DEST_INDEX = 17, TAXI_OUT_INDEX = 20, TAXI_IN_INDEX = 19;
    private static final Integer CODE_INDEX = 0, NAME_INDEX = 1, CITY_INDEX = 2, STATE_INDEX = 3, COUNTRY_INDEX = 4;
    private static final String AIRPORT_FILE_PATH = "airport_file";

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
        private Map<String, String> airportCode;

        @Override
        protected void setup(Context context) throws IOException {
            resultMap = new HashMap<>();
            airportCode = new HashMap<>();
            Path path = new Path(context.getConfiguration().get(AIRPORT_FILE_PATH));
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] lineList = line.split(",");
                    String code = lineList[CODE_INDEX].replaceAll("\"", "").trim();
                    if (code.equalsIgnoreCase("iata")) continue;
                    String name = lineList[NAME_INDEX].replaceAll("\"", "").trim();
                    String city = lineList[CITY_INDEX].replaceAll("\"", "").trim();
                    String state = lineList[STATE_INDEX].replaceAll("\"", "").trim();
                    String country = lineList[COUNTRY_INDEX].replaceAll("\"", "").trim();
                    String airportIdentifier = String.format("%s, %s %s, %s", name, city, state, country);
                    airportCode.put(code, airportIdentifier);
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int count = 0, sum = 0;
            for(IntWritable value : values) {
                sum += value.get();
                count++;
            }
            if(sum > 0 && count > 0) {
                Double avgTaxiTime = (double) (sum) / (double) (count);
                resultMap.put(key.toString(), avgTaxiTime);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<String, Double> shortestTaxi;
            Map<String, Double> longestTaxi;

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
                BigDecimal bd = BigDecimal.valueOf(entry.getValue());
                bd = bd.setScale(4, RoundingMode.HALF_UP);
                String value = String.valueOf(bd.doubleValue());
                if(airportCode.containsKey(entry.getKey())) {
                    context.write(new Text(airportCode.get(entry.getKey())), new Text(value));
                } else {
                    context.write(new Text(entry.getKey()), new Text(value));
                }
                counter++;
                if(counter == 3) break;
            }

            counter = 0;
            context.write(new Text("Airports with Shortest Average Taxi Time"), new Text("Average Time in Minutes"));
            for(Map.Entry<String, Double> entry : shortestTaxi.entrySet()) {
                BigDecimal bd = BigDecimal.valueOf(entry.getValue());
                bd = bd.setScale(4, RoundingMode.HALF_UP);
                String value = String.valueOf(bd.doubleValue());
                if(airportCode.containsKey(entry.getKey())) {
                    context.write(new Text(airportCode.get(entry.getKey())), new Text(value));
                } else {
                    context.write(new Text(entry.getKey()), new Text(value));
                }
                counter++;
                if(counter == 3) break;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 3) {
            System.out.println("expecting 3 arguments, \n1.Input file\n2.Output Directory\n3.Airport CSV");
            System.exit(-1);
        }
        Configuration configuration = new Configuration();
        configuration.set(AIRPORT_FILE_PATH, args[2]);
        Job job = Job.getInstance(configuration, "top 3 airports");
        job.setJarByClass(Top3Airports.class);

        job.setMapperClass(AirportMapper.class);
        job.setReducerClass(AirportReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
