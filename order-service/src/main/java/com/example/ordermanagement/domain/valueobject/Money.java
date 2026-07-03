package com.example.ordermanagement.domain.valueobject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Currency;
import java.util.Objects;

/**
 * Value Object: Money
 *
 * Money is a classic example of a Domain Value Object.
 * Using BigDecimal instead of double/float is critical for financial calculations
 * to avoid floating-point precision errors.
 *
 * This class is immutable — all operations return a new Money instance.
 * This is enforced by the record type and the absence of any setters.
 */
public record Money(BigDecimal amount, Currency currency) {

    public static final Money ZERO = new Money(BigDecimal.ZERO, Currency.getInstance("USD"));

    public Money {
        Objects.requireNonNull(amount, "Amount must not be null");
        Objects.requireNonNull(currency, "Currency must not be null");
        if (amount.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("Money amount cannot be negative: " + amount);
        }
    }

    @JsonCreator
    public static Money of(
            @JsonProperty("amount") BigDecimal amount,
            @JsonProperty("currency") String currencyCode) {
        return new Money(amount.setScale(2, RoundingMode.HALF_UP),
                Currency.getInstance(currencyCode));
    }

    public static Money of(BigDecimal amount) {
        return new Money(amount.setScale(2, RoundingMode.HALF_UP),
                Currency.getInstance("USD"));
    }

    public static Money of(double amount) {
        return of(BigDecimal.valueOf(amount));
    }

    /** Returns a new Money with the sum. Currency must match. */
    public Money add(Money other) {
        assertSameCurrency(other);
        return new Money(this.amount.add(other.amount), this.currency);
    }

    /** Returns a new Money with the difference. Currency must match. */
    public Money subtract(Money other) {
        assertSameCurrency(other);
        return new Money(this.amount.subtract(other.amount), this.currency);
    }

    /** Multiplies by a quantity for line-item calculation */
    public Money multiply(int quantity) {
        return new Money(this.amount.multiply(BigDecimal.valueOf(quantity)), this.currency);
    }

    public boolean isGreaterThan(Money other) {
        assertSameCurrency(other);
        return this.amount.compareTo(other.amount) > 0;
    }

    public boolean isZero() {
        return this.amount.compareTo(BigDecimal.ZERO) == 0;
    }

    private void assertSameCurrency(Money other) {
        if (!this.currency.equals(other.currency)) {
            throw new IllegalArgumentException(
                    "Currency mismatch: " + this.currency + " vs " + other.currency);
        }
    }

    @JsonProperty("currency")
    public String getCurrencyCode() {
        return currency.getCurrencyCode();
    }

    @Override
    public String toString() {
        return currency.getSymbol() + amount.toPlainString();
    }
}
