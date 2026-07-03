package com.example.inventory.infrastructure.persistence;

import com.example.inventory.domain.model.InventoryItem;
import org.springframework.data.repository.CrudRepository;

public interface InventoryItemRepository extends CrudRepository<InventoryItem, String> {
}