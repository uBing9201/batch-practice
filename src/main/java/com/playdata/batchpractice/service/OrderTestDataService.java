package com.playdata.batchpractice.service;

import com.playdata.batchpractice.entity.Order;
import com.playdata.batchpractice.repository.OrderRepository;
import java.time.LocalDateTime;
import java.util.Random;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderTestDataService {

    private final OrderRepository orderRepository;

    public void createTestOrder() {
        orderRepository.deleteAll();

        String[] customers = {"김철수", "이영희", "박민수", "최지원", "정수연", "한승호", "양미래", "임도현", "백지연", "홍길동"};
        Random random = new Random();

        for (int i = 1; i <= 15; i++) {
            String orderNumber = "ORD" + String.format("%05d", i);
            String customer = customers[random.nextInt(customers.length)];
            int amount = (random.nextInt(30) + 1) * 1000; // 1천원~3만원

            LocalDateTime orderDate = LocalDateTime.now()
                    .minusMinutes(random.nextInt(120) + 15); // 15분~2시간 전

            Order order = new Order();
            order.setOrderNumber(orderNumber);
            order.setCustomerName(customer);
            order.setAmount(amount);
            order.setStatus(Order.OrderStatus.PENDING);
            order.setOrderDate(orderDate);

            orderRepository.save(order);

        }
    }
}
