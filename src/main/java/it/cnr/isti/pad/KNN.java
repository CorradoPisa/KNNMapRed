package it.cnr.isti.pad;

import com.sun.tools.javac.util.Pair;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class KNN {
    public static void main(String[] args) throws IOException {
        ArrayList<Pair<ArrayList<Double>, Integer>> train = loadData(new String[]{"src/main/java/trainD.csv"}, false);
        ArrayList<Pair<ArrayList<Double>, Integer>> test = loadData(new String[]{"src/main/java/testD.csv"}, true);
        Integer correct = 0;

        for(Pair<ArrayList<Double>, Integer> testInstance:test){
            ArrayList<Integer> neighbors = neighbors(train,testInstance);
            Integer prediction = vote(neighbors);
            if(prediction == testInstance.snd) correct++;
        }
        System.out.println("Accuracy: " + (double)correct/test.size());
    }

    public static ArrayList<Integer> neighbors(ArrayList<Pair<ArrayList<Double>, Integer>> trainingSet,
                                               Pair<ArrayList<Double>, Integer> testInstance){
        ArrayList<Pair<Integer,Double>> distances = new ArrayList<>();
        for(Pair<ArrayList<Double>, Integer> trainInstance:trainingSet){
            distances.add(new Pair<>(trainInstance.snd, calculateDistance(testInstance.fst, trainInstance.fst)));
        }
        return distances.stream().sorted(Comparator.comparingDouble(o -> o.snd))
                .limit(3).map(x->x.fst).collect(Collectors.toCollection(ArrayList::new));
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

    public static ArrayList<Pair<ArrayList<Double>, Integer>> loadData(String[] path, boolean isTest) throws IOException {
        ArrayList<Pair<ArrayList<Double>, Integer>> digits = new ArrayList<>();
        String line;

        FileReader fileReader = new FileReader(path[0]);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        while ((line = bufferedReader.readLine()) != null) {
            String[] temp = line.split(",");
            ArrayList<Double> digit = new ArrayList<>();
            for(String s : temp){
                digit.add(Double.parseDouble(s));
            }
            if (isTest) digit.remove(0);
            digits.add(new Pair<>(digit,digit.remove(digit.size()-1).intValue()));
        }
        bufferedReader.close();
        return digits;
    }
}