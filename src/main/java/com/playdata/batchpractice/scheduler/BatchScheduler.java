package com.playdata.batchpractice.scheduler;

import java.time.LocalDate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
@EnableScheduling
@RequiredArgsConstructor
@Slf4j
public class BatchScheduler {

    private final JobLauncher jobLauncher;
    private final Job orderProcessJob; // 2단계 기본 주문 잡
    private final Job parameterJob; // 4단계 파라미터 잡

    /*
    ====================
    Cron 표현식 참고:

    초 분 시 일 월 요일 연도
    "0 0 2 * * *"     → 매일 새벽 2시
    "0 0 6 * * MON"   → 매주 월요일 오전 6시
    "0 0 9 1 * *"     → 매월 1일 오전 9시
    fixedRate = 300000 → 5분마다 (300,000ms)
    ====================
     */
    @Scheduled(fixedRate = 30000)
    public void testRun() {
        try {
            LocalDate today = LocalDate.now();

            JobParameters params = new JobParametersBuilder()
                    .addString("startDate", today.minusDays(7).toString())
                    .addString("endDate", today.toString())
                    .addString("minAmount", "7000")
                    .addString("processingMode", "FAST")
                    .addLong("timestamp", System.currentTimeMillis())
                    .toJobParameters();

            JobExecution jobExecution = jobLauncher.run(parameterJob, params);
            log.info("job done!: {}", jobExecution.getStatus());
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

}
