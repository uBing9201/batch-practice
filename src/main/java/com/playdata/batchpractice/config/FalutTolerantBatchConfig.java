package com.playdata.batchpractice.config;

import com.playdata.batchpractice.entity.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDateTime;

/*
====================================
     예외 처리 배치 (Skip, Retry)
====================================

목표: 배치 작업에서 예외 처리 기능 익히기
핵심 개념: Skip, Retry, faultTolerant 설정

이 단계에서 추가되는 내용:
- 에러 발생 상황 시뮬레이션
- Skip 설정으로 에러 건너뛰기
- Retry 설정으로 재시도
*/

@Configuration
@RequiredArgsConstructor
@Slf4j
public class FalutTolerantBatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;


    // 1. ItemReader - 그대로 진행
    @Bean
    public JdbcCursorItemReader<Order> falutTolerantOrderReader() {
        String sql = """
                SELECT id, order_number, customer_name, amount, status, order_date, processed_date
                FROM orders 
                WHERE status = 'PENDING'
                AND order_date < NOW() - INTERVAL 10 MINUTE
                ORDER BY order_date
                """;

        return new JdbcCursorItemReaderBuilder<Order>()
                .name("pendingOrderReader")
                .dataSource(dataSource)
                .sql(sql)
                .rowMapper(new BeanPropertyRowMapper<>(Order.class))
                .build();
    }

    // 2. ItemProcessor - (에러 발생 로직 추가)
    @Bean
    public ItemProcessor<Order, Order> faultTolerantProcessor() {
        return order -> {
            log.info("주문 처리 중: {} (고객: {})", order.getOrderNumber(), order.getCustomerName());

            // 임의로 에러 발생 조건들 추가해 볼게요. (실제 코드에는 해당 서비스에 맞는 예외 조건 추가)

            // 1. 특정 고객명에서 RuntimeException 발생 (Skip 대상)
            if (order.getCustomerName().equals("에러고객")) {
                log.error("RuntimeException 발생: {}", order.getOrderNumber());
                throw new RuntimeException("고객 정보 처리 중 오류 발생!");
            }

            // 2. 음수 금액에서 IllegalArgumentException 발생 (Skip 대상)
            if (order.getAmount() < 0) {
                log.error("IllegalArgumentException 발생!: {}", order.getOrderNumber());
                throw new IllegalArgumentException("주문 금액이 음수입니다!");
            }

            // 3. 특정 주문 번호에서 IllegalStateException 발생 (Retry 대상)
            if (order.getOrderNumber().contains("RETRY")) {
                log.error("IllegalStateException 발생: {}", order.getOrderNumber());
                throw new IllegalStateException("재시도 대상입니다!");
            }


            // 정상 처리
            order.setProcessedDate(LocalDateTime.now()); // 처리 시간 기록

            // 비즈니스 로직: 금액에 따른 처리
            if (order.getAmount() < 10000) {
                order.setStatus(Order.OrderStatus.COMPLETED); // 소액은 즉시 완료
            } else {
                order.setStatus(Order.OrderStatus.PROCESSING); // 일반 주문은 처리 중으로
            }

            log.info("Enum 이름: {}, toString: {}", order.getStatus().name(), order.getStatus().toString());

            return order;
        };
    }

    // 3. ItemWriter - 기존과 동일하게 유지
    @Bean
    public JdbcBatchItemWriter<Order> faultTolerantWriter() {
        JdbcBatchItemWriter<Order> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setSql("""
            UPDATE orders
            SET status = ?, processed_date = ?
            WHERE id = ?
            """);

        writer.setItemPreparedStatementSetter((order, ps) -> {
            ps.setString(1, order.getStatus().toString()); // 또는 getStatus().name() 등 원하는 값
            ps.setObject(2, order.getProcessedDate());
            ps.setLong(3, order.getId());
        });

        writer.afterPropertiesSet();
        return writer;
    }

    // 4. step (예외 처리 기능이 추가된 step)
    @Bean
    public Step faultTolerantStep() {
        return new StepBuilder("faultTolerantStep", jobRepository)
                .<Order, Order>chunk(3, transactionManager)
                .reader(falutTolerantOrderReader())
                .processor(faultTolerantProcessor())
                .writer(faultTolerantWriter())

                // 예외 처리 설정
                .faultTolerant()

                // Skip 설정 - 특정 예외는 건너뛰기
                .skip(RuntimeException.class)
                .skip(IllegalArgumentException.class)
                .skipLimit(10) // 최대 10번까지 Skip 허용

                // Retry - 특정 예외(일시적 오류)는 재시도
                .retry(IllegalStateException.class)
                .retryLimit(3) // 최대 3번까지 재시도

                .build();
    }

    // 5. Job
    @Bean
    public Job faultTolerantJob() {
        return new JobBuilder("falutTolerantJob", jobRepository)
                .start(faultTolerantStep())
                .build();
    }


}