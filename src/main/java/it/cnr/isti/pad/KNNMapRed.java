package it.cnr.isti.pad;

import java.io.IOException;
import java.util.*;
import com.sun.tools.javac.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KNNMapRed {
    private static Configuration conf = new Configuration();

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
            digit.setTestData(row);

            context.write(testInstanceID, digit);
        }
    }

    public static class NewReducer extends Reducer<IntWritable, Digit, IntWritable, IntWritable> {
        private static ArrayList<Pair<ArrayList<Double>, Integer>> trainingSet = loadData(new String[]{"trainD.csv"}, conf);
        private int K = 3;
        // one indicates that prediction was correct
        private final static IntWritable one = new IntWritable(1);
        // zero indicates that prediction was incorrect
        private final static IntWritable zero = new IntWritable(0);
        
        protected void reduce(IntWritable key, Iterable<Digit> values, Context context) throws IOException, InterruptedException {
            int actualValue = -1;
            ArrayList<Digit> digits = new ArrayList<>();
            Digit testDigit = values.iterator().next();

            for (Pair<ArrayList<Double>, Integer> trainInstance : trainingSet) {
                Digit digit = new Digit();
                digit.setDistance(KNN.calculateDistance(testDigit.getTestData(), trainInstance.fst));
                digit.setPrediction(trainInstance.snd);
                digit.setActualValue(testDigit.getActualValue());
                digits.add(digit);
            }

            SortedMap<Double, Integer> top = new TreeMap<>(Comparator.reverseOrder());
            for (Digit digit : digits) {
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
        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}