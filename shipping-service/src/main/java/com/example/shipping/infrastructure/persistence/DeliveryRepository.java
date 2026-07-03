package com.example.shipping.infrastructure.persistence;

import com.example.shipping.domain.model.Delivery;
import org.springframework.data.repository.CrudRepository;

import java.util.Optional;

public interface DeliveryRepository extends CrudRepository<Delivery, String> {

    /** Idempotency check — returns existing delivery if already confirmed. */
    Optional<Delivery> findByShipmentId(String shipmentId);
}