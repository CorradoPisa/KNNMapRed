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

public class KNNMapRed
{
    private static Configuration conf = new Configuration();
    private static ArrayList<Pair<ArrayList<Double>, Integer>> trainingSet = loadData(new String[]{"trainD.csv"}, conf);

    public static class NewMapper extends Mapper<Object, Text, IntWritable, Text> {
        private Text outputVelue = new Text();
        private IntWritable testInstanceID = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] temp = value.toString().split(",");
            ArrayList<Double> digit = new ArrayList<Double>();

            for (String s : temp) {
                digit.add(Double.parseDouble(s));
            }
            Integer actualValue = digit.remove(digit.size() - 1).intValue();
            Integer testID = digit.remove(0).intValue();

            for (Pair<ArrayList<Double>, Integer> trainInstance : trainingSet) {
                testInstanceID.set(testID);
                String distance = String.valueOf(calculateDistance(digit, trainInstance.getKey()));
                outputVelue.set(distance + ',' + trainInstance.getValue() + ',' + actualValue);

                context.write(testInstanceID, outputVelue);
            }
        }
    }

    public static class NewReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
        private int K = 3;
        // one indicates that prediction was correct
        private final static IntWritable one = new IntWritable(1);
        // zero indicates that prediction was incorrect
        private final static IntWritable zero = new IntWritable(0);
        // Does it always print data for same testInstance?
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int actualValue = -1;
            SortedMap<Double, Integer> top = new TreeMap<>((o1, o2) -> o2.compareTo(o1));

            for (Text val : values) {
                String[] temp = val.toString().split(",");
                Double distance = Double.parseDouble(temp[0]);
                int digit = Integer.parseInt(temp[1]);
                actualValue = Integer.parseInt(temp[2]);
                top.put(distance, digit);
                // keep only bottom N
                if (top.size() > K) {
                    top.remove(top.firstKey());
                }
            }
            if (actualValue == vote(top.values())) {
                context.write(key, one);
            } else {
                context.write(key, zero);
            }
        }
    }

    public static double calculateDistance(ArrayList<Double> x, ArrayList<Double> x2) {
        double sum = 0.0;
        for (int i = 0; i < x.size(); i++) {
            sum += Math.pow(x.get(i) - x2.get(i), 2.0);
        }
        return Math.sqrt(sum);
    }

    public static Integer vote(Collection<Integer> neighbors) {
        HashMap<Integer, Integer> classVotes = new HashMap<>();
        for (Integer neighbor : neighbors) {
            Integer count = classVotes.containsKey(neighbor) ? classVotes.get(neighbor) + 1 : 1;
            classVotes.put(neighbor, count);
        }
        return classVotes.entrySet().stream().max((o, o1) -> o.getValue() > o1.getValue() ? 1 : -1).get().getKey();
    }

    public static ArrayList<Pair<ArrayList<Double>, Integer>> loadData(String[] path, Configuration conf) {
        ArrayList<Pair<ArrayList<Double>, Integer>> digits = new ArrayList<>();
        String line;

        Path hadoopPath = new Path(path[0]);
        FSDataInputStream bufferedReader = null;
        try {
            FileSystem fs = FileSystem.get(conf);
            bufferedReader = fs.open(hadoopPath);

            while ((line = bufferedReader.readLine()) != null) {
                String[] temp = line.split(",");
                ArrayList<Double> digit = new ArrayList<>();
                for (String s : temp) {
                    digit.add(Double.parseDouble(s));
                }
                digits.add(new Pair<>(digit, digit.remove(digit.size() - 1).intValue()));
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return digits;
    }

    public static void main(String[] args) throws Exception {
        Job job = new Job(conf, "KNNMapRed");
        job.setJarByClass(KNNMapRed.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(NewMapper.class);
        job.setReducerClass(NewReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}