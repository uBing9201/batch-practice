package com.playdata.batchpractice.controller;

import com.playdata.batchpractice.service.OrderTestDataService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/batch")
@RequiredArgsConstructor
@Slf4j
public class BatchController {

    private final JobLauncher jobLauncher; // 배치 잡 실행기
    private final Job csvToDbJob; // 직접 작성한 배치 작업
    private final Job orderProcessJob; // Order 쪽 빈 등록된 job

    private final OrderTestDataService orderTestDataService;

    @PostMapping("/csv-to-db")
    public String runCsvToJob() {
        try {
            // Spring Batch 는 같은 파라미터로는 한 번만 실행되는 규칙이 있음.
            // 매번 다른 파라미터를 만들면 같은 배치를 여러 번 실행할 수 있다.
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis()) // 현재 시간 추가
                    .toJobParameters();

            log.info("========== CSV To Database 배치 작업 시작! ==========");
            JobExecution jobExecution = jobLauncher.run(csvToDbJob, jobParameters);
            log.info("========== 배치 완료! 상태: {} ==========", jobExecution.getStatus());

            return String.format("배치 실행 완료! 상태: %s, 처리된 아이템 수: %d",
                    jobExecution.getStatus(),
                    jobExecution.getStepExecutions().iterator().next().getWriteCount());

        } catch (Exception e) {
            log.error("배치 실행 중 오류 발생!", e);
            return "배치 실행 실패!" + e.getMessage();
        }
    }

    // 더미데이터 채워넣기 (15개)
    @PostMapping("/setup-orders")
    public String setupOrders() {
        orderTestDataService.createTestOrder();
        return "success";
    }

    @PostMapping("/process-orders")
    public String processOrders() {
        try {
            // Spring Batch 는 같은 파라미터로는 한 번만 실행되는 규칙이 있음.
            // 매번 다른 파라미터를 만들면 같은 배치를 여러 번 실행할 수 있다.
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis()) // 현재 시간 추가
                    .toJobParameters();

            log.info("========== 주문 처리 배치 작업 시작! ==========");
            JobExecution jobExecution = jobLauncher.run(orderProcessJob, jobParameters);
            log.info("========== 배치 완료! 상태: {} ==========", jobExecution.getStatus());

            return String.format("배치 실행 완료! 상태: %s, 처리된 아이템 수: %d",
                    jobExecution.getStatus(),
                    jobExecution.getStepExecutions().iterator().next().getWriteCount());

        } catch (Exception e) {
            log.error("배치 실행 중 오류 발생!", e);
            return "배치 실행 실패!" + e.getMessage();
        }
    }
}
