package mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import static jdk.nashorn.tools.ShellFunctions.input;

public class MapReduce {
        
        public static void main(String[] args) throws FileNotFoundException, InterruptedException, ExecutionException {
                
                // the problem:
                
                // from here (INPUT)
                
                // "file1.txt" => "foo foo bar cat dog dog"
                // "file2.txt" => "foo house cat cat dog"
                // "file3.txt" => "foo foo foo bird"
                
                //get path of the files
                //for the 3 files, read it's content store it in an arraylist
                //TODO: Ask user for filepaths
                String currentPath = Paths.get(".").toAbsolutePath().normalize().toString();
                currentPath = currentPath.substring(0, currentPath.length() - 10);  //change directory down one file
           
                Scanner reader = new Scanner(System.in);
                System.out.println("Number of threads: ");
                int poolSize = reader.nextInt();
                reader.close();
                
                ArrayList<ArrayList> contentArray = new ArrayList<ArrayList>();
                ArrayList<String> doc = new ArrayList<String>();
                for(int i=0; i<3;i++){
                    String filePath = currentPath+"/file"+(i+1)+".txt";
                    System.out.println("FILEPATH: " + filePath);
                    
                    Scanner s = new Scanner(new File(filePath));
                    
                    while (s.hasNext()){
                        doc.add(s.next());
                    }
                    s.close();
                    contentArray.add(doc);                   
                }
                
                 System.out.println("Content Array = " + contentArray.get(0).get(4));
                // we want to go to here (OUTPUT)
                
                // "foo" => { "file1.txt" => 2, "file3.txt" => 3, "file2.txt" => 1 }
                // "bar" => { "file1.txt" => 1 }
                // "cat" => { "file2.txt" => 2, "file1.txt" => 1 }
                // "dog" => { "file2.txt" => 1, "file1.txt" => 2 }
                // "house" => { "file2.txt" => 1 }
                // "bird" => { "file3.txt" => 1 }
                
                // in plain English we want to
                
                // Given a set of files with contents
                // we want to index them by word 
                // so I can return all files that contain a given word
                // together with the number of occurrences of that word
                // without any sorting
                
                ////////////
                // INPUT:
                ///////////
                
                Map<String, ArrayList> input = new HashMap<String, ArrayList>();
                input.put("file1.txt", contentArray);
//                input.put("file2.txt", "foo house cat cat dog");
//                input.put("file3.txt", "foo foo foo bird");
                // APPROACH #1: Brute force
                {
                        System.out.println("Approach 1:");
                        Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
                        
                        Iterator<Map.Entry<String, ArrayList>> inputIter = input.entrySet().iterator();
                        while(inputIter.hasNext()) {
                                Map.Entry<String, ArrayList> entry = inputIter.next();
                                String file = entry.getKey();
                                String contents = entry.getValue().toString();  
                                
                               String[] words = contents.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
                               // String[] words = contents.trim().split("\\s+");
//                                for(int i=0; i<contents.size(); i++){
                                 for(String word : words) {
//                                
                                        
                                        Map<String, Integer> files = output.get(word);
                                        if (files == null) {
                                                files = new HashMap<String, Integer>();
                                                output.put(word, files);
                                        }
                                        
                                        Integer occurrences = files.remove(file);
                                        if (occurrences == null) {
                                                files.put(file, 1);
                                        } else {
                                                files.put(file, occurrences.intValue() + 1);
                                        }
                                }
                               
                        }
                        
                        // show me:
                        System.out.println(output);
                }

                
//                // APPROACH #2: MapReduce
                {
                        System.out.println("Approach 2:");
                        Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
                        
                        // MAP:
                        
                        List<MappedItem> mappedItems = new LinkedList<MappedItem>();
                        
                         Iterator<Map.Entry<String, ArrayList>> inputIter = input.entrySet().iterator();
                        while(inputIter.hasNext()) {
                                Map.Entry<String, ArrayList> entry = inputIter.next();
                                String file = entry.getKey();
                                String contents = entry.getValue().toString();
                                
                                map(file, contents, mappedItems);
                        }
                        
                        // GROUP:
                        
                        Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
                        
                        Iterator<MappedItem> mappedIter = mappedItems.iterator();
                        while(mappedIter.hasNext()) {
                                MappedItem item = mappedIter.next();
                                String word = item.getWord();
                                String file = item.getFile();
                                List<String> list = groupedItems.get(word);
                                if (list == null) {
                                        list = new LinkedList<String>();
                                        groupedItems.put(word, list);
                                }
                                list.add(file);
                        }
                        
                        // REDUCE:
                        
                        Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
                        while(groupedIter.hasNext()) {
                                Map.Entry<String, List<String>> entry = groupedIter.next();
                                String word = entry.getKey();
                                List<String> list = entry.getValue();
                                
                                reduce(word, list, output);
                        }
                        
                        System.out.println(output);
                }
                
                
//                // APPROACH #3: Distributed MapReduce
                {       System.out.println("Approach 3:");
                        
                        long startTime = System.nanoTime();     //Start timer
                
                        final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
                       // Create thread pool
                        ExecutorService executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(poolSize);
                        
                        // MAP:
                        
                        final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
                        
                        final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                                @Override
                public synchronized void mapDone(String file, List<MappedItem> results) {
                    mappedItems.addAll(results);
                }
                        };
                        
                        List<Thread> mapCluster = new ArrayList<Thread>(input.size());  

                        Iterator<Map.Entry<String, ArrayList>> inputIter = input.entrySet().iterator();
                        while(inputIter.hasNext()) {
                                Map.Entry<String, ArrayList> entry = inputIter.next();
  
                                final String file = entry.getKey(); 
                                final String contents = entry.getValue().toString();

                                
                                Thread t = new Thread(new Runnable() {
                                        @Override
                                        public void run() {
                                           // System.out.println("Inside map thread");
                                            map(file, contents, mapCallback);
                                            
                                        }
                                });
                                mapCluster.add(t);
                                Future future = executor.submit(t);
                                future.get(); //TODO: Should I block the thread here??
                              //  t.start();
                        }
                        
                        
                        // wait for mapping phase to be over:
                        for(Thread t : mapCluster) {
                                try {
                                        t.join();
                                } catch(InterruptedException e) {
                                        throw new RuntimeException(e);
                                }
                        }
                        
                        // GROUP:
                        
                        Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
                        
                        Iterator<MappedItem> mappedIter = mappedItems.iterator();
                        while(mappedIter.hasNext()) {
                                MappedItem item = mappedIter.next();
                                String word = item.getWord();
                                String file = item.getFile();
                                List<String> list = groupedItems.get(word);
                                if (list == null) {
                                        list = new LinkedList<String>();
                                        groupedItems.put(word, list);
                                }
                                list.add(file);
                        }
                        
                        // REDUCE:
                        
                        final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                                @Override
                public synchronized void reduceDone(String k, Map<String, Integer> v) {
                        output.put(k, v);
                }
                        };
                        
                        List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());
                        
                        Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
                        while(groupedIter.hasNext()) {
                                Map.Entry<String, List<String>> entry = groupedIter.next();
                                final String word = entry.getKey();
                                final List<String> list = entry.getValue();
                                
                                Thread t = new Thread(new Runnable() {
                                        @Override
                                    public void run() {         //System.out.println("Inside reduce thread");
                                        reduce(word, list, reduceCallback);
                                    }
                                });
                                reduceCluster.add(t);
                                Future future = executor.submit(t);
                                future.get();
                                //t.start();
                        }
                        
                        // wait for reducing phase to be over:
                        for(Thread t : reduceCluster) {
                                try {
                                        t.join();
                                } catch(InterruptedException e) {
                                        throw new RuntimeException(e);
                                }
                        }
                        
                        System.out.println(output);
                        executor.shutdown();
                        
                        while (!executor.isTerminated()) {
        }
        long endTime = System.nanoTime();   //finish timer count
        
        System.out.println("Finished all threads");
        
        

        long duration = (endTime - startTime);
        System.out.println("The total runtime of approach 3: "+duration);
                }
        }
        
        public static void map(String file, String contents, List<MappedItem> mappedItems) {
                String[] words = contents.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
                for(String word: words) {
                        mappedItems.add(new MappedItem(word, file));
                }
        }
        
        public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
                Map<String, Integer> reducedList = new HashMap<String, Integer>();
                for(String file: list) {
                        Integer occurrences = reducedList.get(file);
                        if (occurrences == null) {
                                reducedList.put(file, 1);
                        } else {
                                reducedList.put(file, occurrences.intValue() + 1);
                        }
                }
                output.put(word, reducedList);
        }
        
        public static interface MapCallback<E, V> {
                
                public void mapDone(E key, List<V> values);
        }
        
        public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
               String[] words = contents.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
                List<MappedItem> results = new ArrayList<MappedItem>(words.length);
                for(String word: words) {
                        results.add(new MappedItem(word, file));
                }
                callback.mapDone(file, results);
        }
        
        public static interface ReduceCallback<E, K, V> {
                
                public void reduceDone(E e, Map<K,V> results);
        }
        
        public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {
                
                Map<String, Integer> reducedList = new HashMap<String, Integer>();
                for(String file: list) {
                        Integer occurrences = reducedList.get(file);
                        if (occurrences == null) {
                                reducedList.put(file, 1);
                        } else {
                                reducedList.put(file, occurrences.intValue() + 1);
                        }
                }
                callback.reduceDone(word, reducedList);
        }
        
        private static class MappedItem { 
                
                private final String word;
                private final String file;
                
                public MappedItem(String word, String file) {
                        this.word = word;
                        this.file = file;
                }

                public String getWord() {
                        return word;
                }

                public String getFile() {
                        return file;
                }
                
                @Override
                public String toString() {
                        return "[\"" + word + "\",\"" + file + "\"]";
                }
        }
} 
