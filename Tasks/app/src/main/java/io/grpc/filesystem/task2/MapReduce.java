/*
 * the MapReduce functionality implemeted in this program takes a single large text file to map i.e. split it into small chunks and then assign 1 to all the found words
 * then reduces by adding count values to each unique words
 * To build: ./gradlew build
 * To run: ./gradlew run -PchooseMain=io.grpc.filesystem.task2.MapReduce --args="input/pigs.txt output/output-task2.txt"
 */

package io.grpc.filesystem.task2;

import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.io.*;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Map;

public class MapReduce {
    private static final Logger LOGGER = Logger.getLogger(MapReduce.class.getName());

    public static String makeChunks(String inputFilePath) throws IOException {
        int count = 1;
        int size = 500;
        File f = new File(inputFilePath);
        try (BufferedReader br = new BufferedReader(new FileReader(inputFilePath))) {
            String l = br.readLine();

            while (l != null) {
                File newFile = new File(f.getParent() + "/temp", "chunk"
                        + String.format("%03d", count++) + ".txt");
                try (OutputStream out = new BufferedOutputStream(new FileOutputStream(newFile))) {
                    int fileSize = 0;
                    while (l != null) {
                        byte[] bytes = (l + System.lineSeparator()).getBytes(Charset.defaultCharset());
                        if (fileSize + bytes.length > size)
                            break;
                        out.write(bytes);
                        fileSize += bytes.length;
                        l = br.readLine();
                    }
                }
            }
        }
        return f.getParent() + "/temp";

    }

    /**
     * @param inputfilepath
     * @return
     * @throws IOException
     */
    public static String map(String inputfilepath) throws IOException {
        File chunkFile = new File(inputfilepath);
        File mapOutputDir = new File(chunkFile.getParent(), "map");
        if (!mapOutputDir.exists()) {
            mapOutputDir.mkdirs();
        }
        File mapOutputFile = new File(mapOutputDir, "map-" + chunkFile.getName());

        try (Scanner scanner = new Scanner(chunkFile);
             BufferedWriter writer = new BufferedWriter(new FileWriter(mapOutputFile))) {
            while (scanner.hasNext()) { // Use hasNext() and next() for word by word processing
                String word = scanner.next().replaceAll("\\p{Punct}", "").toLowerCase();
                if (word.matches("^[a-zA-Z0-9]+$")) {
                    writer.write(word + ":1");
                    writer.newLine();
                }
            }
        }
        return inputfilepath;
    }

    /**
     * @param inputfilepath
     * @param outputfilepath
     * @return
     * @throws IOException
     */
    public static String reduce(String inputfilepath, String outputfilepath) throws IOException {
        LOGGER.info("Starting the reduce process...");
        Map<String, Integer> reduceResults = new HashMap<>();

        File dir = new File(inputfilepath + "/map");
        File[] mapFiles = dir.listFiles();

        if (mapFiles != null) {
            LOGGER.info("Number of map files found: " + mapFiles.length);
            for (File mapFile : mapFiles) {
                try (Scanner scanner = new Scanner(mapFile)) {
                    while (scanner.hasNextLine()) {
                        String[] parts = scanner.nextLine().split(":");
                        String word = parts[0];
                        int count = Integer.parseInt(parts[1]);

                        reduceResults.put(word, reduceResults.getOrDefault(word, 0) + count);
                    }
                }
            }
        } else {
            LOGGER.warning("No map files found in directory: " + dir.getAbsolutePath());
            return inputfilepath;
        }

        // Sort the results
        Map<String, Integer> sortedResults = reduceResults.entrySet().stream()
                .sorted(Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        // Save the reduce results to file
        try (PrintWriter writer = new PrintWriter(new File(outputfilepath))) {
            for (Entry<String, Integer> entry : sortedResults.entrySet()) {
                writer.println(entry.getKey() + ":" + entry.getValue());
            }
        }
        LOGGER.info("Reduce process completed.");
        return inputfilepath;
    }


    /**
     * Takes a text file as an input and returns counts of each word in a text file
     * "output-task2.txt"
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException { // update the main function if required
        String inputFilePath = args[0];
        String outputFilePath = args[1];
        String chunkpath = makeChunks(inputFilePath);
        File dir = new File(chunkpath);
        File[] directoyListing = dir.listFiles();
        if (directoyListing != null) {
            for (File f : directoyListing) {
                if (f.isFile()) {

                    map(f.getPath());

                }

            }

            reduce(chunkpath, outputFilePath);

        }

    }
}