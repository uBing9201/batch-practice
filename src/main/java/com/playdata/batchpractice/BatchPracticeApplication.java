package com.playdata.batchpractice;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class BatchPracticeApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchPracticeApplication.class, args);
    }

}
