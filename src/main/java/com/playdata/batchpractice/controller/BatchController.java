package com.playdata.batchpractice.controller;

import com.playdata.batchpractice.service.OrderTestDataService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
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
    private final Job csvToDbJob; // 직접 작성한 배치 작업 (빈등록 해놓음)
    private final Job orderProcessJob; // Order쪽 빈 등록된
    private final Job faultTolerantJob;
    private final OrderTestDataService orderTestDataService;

    @PostMapping("/csv-to-db")
    public String runCsvToDbJob() {
        try {
            // Spring Batch는 같은 파라미터로는 한 번만 실행되는 규칙이 있음.
            // 매번 다른 파라미터를 만들면 같은 배치를 여러 번 실행할 수 있습니다.
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp", System.currentTimeMillis()) // 현재 시간 추가
                    .toJobParameters();

            log.info(" ========== CSV To Database 배치 작업 시작! =========");
            JobExecution jobExecution = jobLauncher.run(csvToDbJob, jobParameters);
            log.info(" ========== 배치 완료! 상태: {} =========", jobExecution.getStatus());

            return String.format("배치 실행 완료! 상태: %s, 처리된 아이템 수: %d",
                    jobExecution.getStatus(),
                    jobExecution.getStepExecutions().iterator().next().getWriteCount());

        } catch (Exception e) {
            log.error("배치 실행 중 오류 발생!", e);
            return "배치 실행 실패!: " + e.getMessage();
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
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp", System.currentTimeMillis()) // 현재 시간 추가
                    .toJobParameters();

            log.info(" ========== 주문 처리 배치 작업 시작! =========");
            JobExecution jobExecution = jobLauncher.run(orderProcessJob, jobParameters);
            log.info(" ========== 배치 완료! 상태: {} =========", jobExecution.getStatus());

            return String.format("배치 실행 완료! 상태: %s, 처리된 아이템 수: %d",
                    jobExecution.getStatus(),
                    jobExecution.getStepExecutions().iterator().next().getWriteCount());

        } catch (Exception e) {
            log.error("배치 실행 중 오류 발생!", e);
            return "배치 실행 실패!: " + e.getMessage();
        }
    }

    @PostMapping("/fault-torelant")
    public String runFaultTorelant() {
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("timestamp", System.currentTimeMillis()) // 현재 시간 추가
                    .toJobParameters();

            log.info(" ========== 예외 처리 배치 작업 시작! =========");
            JobExecution jobExecution = jobLauncher.run(faultTolerantJob, jobParameters);
            log.info(" ========== 배치 완료! 상태: {} =========", jobExecution.getStatus());

            return String.format("배치 실행 완료! 상태: %s, 처리된 아이템 수: %d건, Skip: %d건",
                    jobExecution.getStatus(),
                    jobExecution.getStepExecutions().iterator().next().getWriteCount(),
                    jobExecution.getStepExecutions().iterator().next().getSkipCount());

        } catch (Exception e) {
            log.error("배치 실행 중 오류 발생!", e);
            return "배치 실행 실패!: " + e.getMessage();
        }
    }


}