package org.example;

public class Main {
    public static void main(String[] args) {

        ParserMerger parserMerger = new ParserMerger(args[0], 5, "inverted_index.txt");

        Thread parser = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    parserMerger.parse();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Thread merger = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    parserMerger.merge();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        parser.start();
        merger.start();
    }
}