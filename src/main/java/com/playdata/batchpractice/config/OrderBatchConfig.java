package com.playdata.batchpractice.config;

import com.playdata.batchpractice.entity.Order;
import com.playdata.batchpractice.entity.Order.OrderStatus;
import java.time.LocalDateTime;
import javax.sql.DataSource;
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
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

/*
====================================
       DB → DB 배치 (주문 처리)
====================================

목표: 데이터베이스의 주문 정보를 읽어서 처리하는 배치
핵심 개념: JdbcCursorItemReader, ItemProcessor, 비즈니스 로직 적용

이전 단계에서 추가되는 내용:
- Order 엔티티 추가
- DB → DB 처리
- ItemProcessor로 비즈니스 로직 적용
*/

@Configuration
@RequiredArgsConstructor
@Slf4j
public class OrderBatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;

    // 1. ItemReader - PENDING 상태 주문들을 DB에서 조회
    @Bean
    public JdbcCursorItemReader<Order> pendingOrderReader() {
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

    // 2. ItemProcessor - 주문 처리 로직
    @Bean
    public ItemProcessor<Order, Order> orderProcessor() {
        return order -> {
            log.info("주문 처리 중: {} (고객: {})", order.getOrderNumber(), order.getCustomerName());

            order.setProcessedDate(LocalDateTime.now()); // 처리 시간 기록

            // 비즈니스 로직: 금액에 따른 처리
            if (order.getAmount() < 10000) {
                order.setStatus(OrderStatus.COMPLETED); // 소액은 즉시 완료
            } else {
                order.setStatus(OrderStatus.PROCESSING);
            }

            return order;
        };
    }

    // 3. ItemWriter - 데이터베이스 업데이트
    @Bean
    public JdbcBatchItemWriter<Order> orderWriter() {
//        return new JdbcBatchItemWriterBuilder<Order>()
//                .dataSource(dataSource)
//                .sql(
//                        """
//                        UPDATE orders
//                        SET status = ?, processed_date = ?
//                        WHERE id = ?
//                        """
//                )
//                .itemPreparedStatementSetter((order, ps) -> {
//                    ps.setString(1, order.getStatus().name()); // enum -> String
//                    ps.setObject(2, order.getProcessedDate());
//                    ps.setLong(3, order.getId());
//                })
//                .build();

        // beanMapped() 를 홀용해서 Order 객체의 status를 sql에 채워넣으려 햇는데,
        // status의 toString 을 호출할 수 없어서 맞지 않는 값이 update에 전달
        // 수동으로 sql에 들어갈 값을 직접 채워 넣음
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

    // 4. step
    @Bean
    public Step orderProcessStep() {
        return new StepBuilder("orderProcessStep", jobRepository)
                .<Order, Order>chunk(5, transactionManager)
                .reader(pendingOrderReader())
                .processor(orderProcessor())
                .writer(orderWriter())
                .build();
    }

    // 5. Job
    @Bean
    public Job orderProcessJob() {
        return new JobBuilder("orderProcessJob", jobRepository)
                .start(orderProcessStep())
//                .next(orderProcessStep()) // Step 가 더 있다면 next 로 추가
                .build();
    }

}
