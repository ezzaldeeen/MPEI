package org.example;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ParserMerger {

    // the maximum capacity in the buffer (i.e. max number of terms)
    private final int MAX_CAPACITY;
    private int currentCapacity = 0;
    private final HashMap<String, Integer> fileStatus = new HashMap<>();
    private final List<HashMap<String, List<String>>> buffers = new ArrayList<>();
    // the absolute path for the sub-inverted indices
    private final String DIR_PATH;

    public ParserMerger(String directoryPath, int capacity) {
        this.MAX_CAPACITY = capacity;
        this.DIR_PATH = directoryPath;

        File[] files = getFiles();
        for (File file : files) {
            // store the abs path as key, and the line index for the file
            // in order to keep tracking the status of the file
            fileStatus.put(file.getAbsolutePath(), 0);
        }
    }

    private File[] getFiles() {
        File folder = new File(this.DIR_PATH);
        return folder.listFiles();
    }

    private boolean isFinished() {
        for (int lineNum : fileStatus.values())
            if (lineNum >= 0) {
                return false;
            }
        return true;
    }

    private String getLine(String absPath,
                           int lineNum) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(absPath));
        if (lines.size() > lineNum && lineNum >= 0)
            return lines.get(lineNum);
        return null;
    }

    private void writeToFile(HashMap<String, List<String>> invertedIndex,
                             String path) {
        try {
            List<String> orderedTerms = new ArrayList<>(invertedIndex.keySet());
            FileWriter writer = new FileWriter(path);
            Collections.sort(orderedTerms);

            for (String term : orderedTerms) {
                int count = invertedIndex.get(term).size();
                List<String> postingList = invertedIndex.get(term);
                writer.write(term + "," + count);
                for (String docId : postingList)
                    writer.write("," + docId);
                writer.write("\n");
            }
            writer.close();

        } catch (IOException exception) {
            System.out.println(exception);
        }

    }

    private List<String> splitLine(String line) {
        return Arrays.asList(line.split(","));
    }

    public void parse() throws InterruptedException {
        while (!isFinished()) {
            synchronized (this) {
                while (currentCapacity == MAX_CAPACITY)
                    wait();
                notify();

                File[] files = this.getFiles();

                for (File file : files) {
                    HashMap<String, List<String>> buffer = new HashMap<>();
                    try {
                        String absolutePath = file.getAbsolutePath();
                        String fileName = file.getName();
                        while (buffer.size() < MAX_CAPACITY) {
                            // get the last line index that read
                            int lineNum = fileStatus.get(absolutePath);

                            String line = getLine(absolutePath, lineNum);
                            // if all the terms have parsed from the file
                            if (line == null) {
                                fileStatus.put(absolutePath, -1);
                                break;
                            }
                            // update the line index for the file, to keep tracking
                            fileStatus.put(absolutePath, lineNum + 1);

                            List<String> splittedLine = splitLine(line);
                            // extract entities from the line
                            String term = splittedLine.get(0);
                            int count = Integer.parseInt(splittedLine.get(1));
                            // extract the documents IDs from the line
                            List<String> postingList = splittedLine.subList(2, count + 2);

                            buffer.put(term, postingList);
                        }
                        buffers.add(buffer);
                        currentCapacity++;

                    } catch (IOException exception) {
                        System.out.println(
                                "File: " + file.getName() + " with exception. " + exception
                        );
                    } catch (IndexOutOfBoundsException exception) {
                        System.out.println(
                                "The count for doesn't match with the number of documents" +
                                        "in the file: " + file.getName()
                        );
                        throw exception;
                    }
                }
            }
        }
    }

    public void merge() throws InterruptedException {
        while (true) {
            HashMap<String, List<String>> mergedInvIndex = new HashMap<>();
            synchronized (this) {
                while (currentCapacity == 0) {
                    wait();
                }
                notify();

                for (HashMap<String, List<String>> buffer : buffers) {
                    for (Map.Entry<String, List<String>> entry : buffer.entrySet()) {
                        String term = entry.getKey();
                        if (!mergedInvIndex.containsKey(term))
                            mergedInvIndex.put(term, entry.getValue());

                        List<String> existedPostingList = mergedInvIndex.get(term);
                        List<String> currentPostingList = entry.getValue();

                        Set<String> mergedPostingList = new HashSet<>();
                        mergedPostingList.addAll(existedPostingList);
                        mergedPostingList.addAll(currentPostingList);

                        List<String> updatedPostingList = new ArrayList<>(mergedPostingList);
                        Collections.sort(updatedPostingList);

                        mergedInvIndex.put(term, updatedPostingList);
                    }
                }
                currentCapacity = 0;
            }
            if (isFinished()) {
                writeToFile(mergedInvIndex, "inverted_index.txt");
                System.out.println("MERGED!");
                return;
            }
        }
    }
}
