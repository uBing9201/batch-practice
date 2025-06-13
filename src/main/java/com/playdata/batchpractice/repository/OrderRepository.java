package com.playdata.batchpractice.repository;

import com.playdata.batchpractice.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {

}
