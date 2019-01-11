package it.cnr.isti.pad;

import java.io.IOException;
import javafx.util.Pair;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KNNMapRed {
    private static Configuration conf = new Configuration();
    private static ArrayList<Pair<ArrayList<Double>, Integer>> trainingSet = loadData(new String[]{"trainD.csv"}, conf);

    public static class NewMapper extends Mapper<Object, Text, IntWritable, Digit> {
        private IntWritable testInstanceID = new IntWritable();
        private Digit digit = new Digit();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] temp = value.toString().split(",");
            ArrayList<Double> row = new ArrayList<Double>();

            for (String s : temp) {
                row.add(Double.parseDouble(s));
            }
            // ID of a digit is prepended to each row
            testInstanceID.set(row.remove(0).intValue());
            // Actual value is last number in the row
            digit.setActualValue(row.remove(row.size() - 1).intValue());

            for (Pair<ArrayList<Double>, Integer> trainInstance : trainingSet) {
                digit.setDistance(KNN.calculateDistance(row, trainInstance.getKey()));
                digit.setPrediction(trainInstance.getValue());

                context.write(testInstanceID, digit);
            }
        }
    }

    public static class NewReducer extends Reducer<IntWritable, Digit, IntWritable, IntWritable> {
        private int K = 3;
        // one indicates that prediction was correct
        private final static IntWritable one = new IntWritable(1);
        // zero indicates that prediction was incorrect
        private final static IntWritable zero = new IntWritable(0);

        public void reduce(IntWritable key, Iterable<Digit> values, Context context) throws IOException, InterruptedException {
            int actualValue = -1;
            SortedMap<Double, Integer> top = new TreeMap<>(Comparator.reverseOrder());

            for (Digit digit : values) {
                actualValue = digit.getActualValue();
                top.put(digit.getDistance(), digit.getPrediction());
                if (top.size() > K) {
                    top.remove(top.firstKey());
                }
            }
            if (actualValue == KNN.vote(top.values())) {
                context.write(key, one);
            } else {
                context.write(key, zero);
            }
        }
    }

    public static ArrayList<Pair<ArrayList<Double>, Integer>> loadData(String[] path, Configuration conf) {
        ArrayList<Pair<ArrayList<Double>, Integer>> rows = new ArrayList<>();
        String line;
        Path hadoopPath = new Path(path[0]);
        FSDataInputStream bufferedReader = null;
        try {
            FileSystem fs = FileSystem.get(conf);
            bufferedReader = fs.open(hadoopPath);

            while ((line = bufferedReader.readLine()) != null) {
                String[] temp = line.split(",");
                ArrayList<Double> row = new ArrayList<>();
                for (String s : temp) {
                    row.add(Double.parseDouble(s));
                }
                rows.add(new Pair<>(row, row.remove(row.size() - 1).intValue()));
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rows;
    }

    public static void main(String[] args) throws Exception {
        Job job = new Job(conf, "KNNMapRed");
        job.setJarByClass(KNNMapRed.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Digit.class);
        job.setMapperClass(NewMapper.class);
        job.setReducerClass(NewReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}