package com.playdata.batchpractice.config;

import com.playdata.batchpractice.entity.User;
import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;

/*
====================================
     기본 배치 개념 익히기
====================================

목표: CSV 파일을 읽어서 데이터베이스에 저장하는 가장 기본적인 배치
핵심 개념: Job, Step, ItemReader, ItemWriter, Chunk 처리

실행 순서:
1. CSV 파일 준비
2. 배치 실행
3. 결과 확인
*/

@Configuration
@RequiredArgsConstructor
@Slf4j
public class BasicBatchConfig {

    // 배치 관련 메타데이터 저장소 (실행 정보)
    private final JobRepository jobRepository;
    // 배치에서 사용하는 트랜잭션 관리자
    private final PlatformTransactionManager transactionManager;
    // 데이터베이스 연결 정보
    private final DataSource dataSource;

    // 데이터 읽기 (ItemReader)
    @Bean
    public FlatFileItemReader<User> userCsvReader() {
        return new FlatFileItemReaderBuilder<User>()
                .name("userCsvReader") // 이름 지어주기
                // 어떤 파일을 읽을것인가
                // ClassPathResource: src/main/resources 경로에 있는 파일을 읽어들이는 객체
                .resource(new ClassPathResource("users.csv"))
                .delimited() // 쉼표로 구분된 csv 파일입니다.
                .names("id", "name", "email", "age", "city") // 컬럼 정보
                .fieldSetMapper(
                        new BeanWrapperFieldSetMapper<>() {{
                            setTargetType(User.class); // 읽어들인 데이터를 User 객체로 변환
                        }}
                )
                .linesToSkip(1) // 첫 줄 (헤더) 건너뛰기
                .build();
    }

    // 데이터 저장하기 (ItemWriter
    @Bean
    public JdbcBatchItemWriter<User> userDbWriter() {
        return new JdbcBatchItemWriterBuilder<User>()
                .itemSqlParameterSourceProvider(BeanPropertySqlParameterSource::new)
                .sql("INSERT INTO users (name, email, age, city) VALUES (:name, :email, :age, :city)")
                .dataSource(dataSource) // 데이터베이스 정보 전달
                .build();

    }

    // 작업단계 만들기 (step)
    @Bean
    public Step csvToDbStep() {
        return new StepBuilder("csvToDbStep", jobRepository)
                // <User, User>: Reader에서 읽어온 타입과 Writer로 전달하는 데이터 타입 명시
                // chunk: step이 작업을 처리할 때 기준에 맞춰 나눠서 작업을 처리.
                // chunk(10): 10개씩 묵어서 처리, 단위별로 작업 후 commit, 문제가 있다면 rollback
                // 단위를 나눠놓지 않으면 전체 데이터가 rollback 되기 때문에, 작은 단위로 나눠 작업을 진행
                .<User, User>chunk(10, transactionManager)
                .reader(userCsvReader())
                .writer(userDbWriter())
                .build();
    }

    // 전체 작업 정의하기 (Job)
    @Bean
    public Job csvToDbJob() {
        return new JobBuilder("csvToDbJob", jobRepository)
                .start(csvToDbStep())
                .build();
    }
}
