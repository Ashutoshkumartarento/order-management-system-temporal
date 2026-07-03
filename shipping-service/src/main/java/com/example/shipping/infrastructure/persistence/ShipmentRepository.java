package com.example.shipping.infrastructure.persistence;

import com.example.shipping.domain.model.Shipment;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.List;

/**
 * Spring Data JDBC Repository for Shipment persistence.
 */
@Repository
public interface ShipmentRepository extends CrudRepository<Shipment, String> {
    /**
     * Find the shipment for an order.
     * Idempotency: if already shipped, return existing.
     */
    Optional<Shipment> findByOrderId(String orderId);

    /**
     * Find all shipments for an order.
     */
    List<Shipment> findAllByOrderId(String orderId);
}
