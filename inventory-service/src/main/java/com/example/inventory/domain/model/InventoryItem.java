package com.example.inventory.domain.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Table("inventory_items")
public class InventoryItem implements Persistable<String> {

    @Id
    private final String productId;
    private final String productName;
    private final int quantityAvailable;
    private final int quantityReserved;
    private final Instant updatedAt;

    @Transient
    private final boolean isNew;

    /** Used by Spring Data JDBC when loading rows from DB — always an UPDATE on next save. */
    @PersistenceConstructor
    public InventoryItem(String productId, String productName,
                         int quantityAvailable, int quantityReserved,
                         Instant updatedAt) {
        this.productId = productId;
        this.productName = productName;
        this.quantityAvailable = quantityAvailable;
        this.quantityReserved = quantityReserved;
        this.updatedAt = updatedAt;
        this.isNew = false;
    }

    private InventoryItem(String productId, String productName,
                          int quantityAvailable, int quantityReserved,
                          Instant updatedAt, boolean isNew) {
        this.productId = productId;
        this.productName = productName;
        this.quantityAvailable = quantityAvailable;
        this.quantityReserved = quantityReserved;
        this.updatedAt = updatedAt;
        this.isNew = isNew;
    }

    /** Factory for brand-new items — Spring Data JDBC will INSERT rather than UPDATE. */
    public static InventoryItem create(String productId, String productName, int quantity) {
        return new InventoryItem(productId, productName, quantity, 0, Instant.now(), true);
    }

    public boolean hasStock(int quantity) {
        return quantityAvailable >= quantity;
    }

    /** Returns a new instance with stock decremented by quantity. */
    public InventoryItem reserve(int quantity) {
        if (!hasStock(quantity)) {
            throw new InsufficientStockException(
                "Insufficient stock for product " + productId +
                ": requested=" + quantity + ", available=" + quantityAvailable);
        }
        return new InventoryItem(productId, productName,
                quantityAvailable - quantity,
                quantityReserved + quantity,
                Instant.now());
    }

    /** Returns a new instance with stock restored by quantity. */
    public InventoryItem release(int quantity) {
        return new InventoryItem(productId, productName,
                quantityAvailable + quantity,
                Math.max(0, quantityReserved - quantity),
                Instant.now());
    }

    @Override public String getId()    { return productId; }
    @Override public boolean isNew()   { return isNew; }

    public String  productId()          { return productId; }
    public String  productName()        { return productName; }
    public int     quantityAvailable()  { return quantityAvailable; }
    public int     quantityReserved()   { return quantityReserved; }
    public Instant updatedAt()          { return updatedAt; }
}