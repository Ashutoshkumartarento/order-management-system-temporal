package com.example.payment.infrastructure.persistence;

import com.example.payment.domain.model.Transaction;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.List;

/**
 * Spring Data JDBC Repository for Transaction persistence.
 */
@Repository
public interface TransactionRepository extends CrudRepository<Transaction, String> {
    /**
     * Find the most recent charge transaction for an order.
     * Used for idempotency checks.
     */
    Optional<Transaction> findFirstByOrderIdAndTypeOrderByCreatedAtDesc(String orderId, String type);

    /**
     * Find all transactions for an order.
     */
    List<Transaction> findByOrderId(String orderId);
}
