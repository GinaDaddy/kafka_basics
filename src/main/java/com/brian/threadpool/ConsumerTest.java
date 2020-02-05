package com.brian.threadpool;

public class ConsumerTest {

    public static void main(String[] args) {
        KafkaProcessor processor = new KafkaProcessor();
        try {
            processor.init(5);
        } catch (Exception e) {
            processor.shutdown();
        }
    }
}
