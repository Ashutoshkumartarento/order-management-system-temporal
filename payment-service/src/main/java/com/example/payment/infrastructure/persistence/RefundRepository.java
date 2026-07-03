package com.example.payment.infrastructure.persistence;

import com.example.payment.domain.model.Refund;
import org.springframework.data.repository.CrudRepository;

import java.util.Optional;

public interface RefundRepository extends CrudRepository<Refund, String> {

    /** Idempotency check — returns existing refund if this charge was already refunded. */
    Optional<Refund> findByOriginalTransactionId(String originalTransactionId);
}