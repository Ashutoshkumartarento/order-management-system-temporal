package com.example.ordermanagement.infrastructure.projection;

import com.example.ordermanagement.api.dto.response.OrderSummaryResponse;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@Repository
public class OrderSummaryRepository {

    private final NamedParameterJdbcTemplate jdbc;

    public OrderSummaryRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(String orderId, String customerId, String shippingAddress, Instant createdAt) {
        jdbc.update(INSERT_SQL, new MapSqlParameterSource()
                .addValue("orderId", orderId)
                .addValue("customerId", customerId)
                .addValue("shippingAddress", shippingAddress)
                .addValue("createdAt", Timestamp.from(createdAt))
                .addValue("updatedAt", Timestamp.from(createdAt)));
    }

    public void incrementItemCount(String orderId, Instant updatedAt) {
        jdbc.update("UPDATE order_summary SET item_count = item_count + 1, updated_at = :u WHERE order_id = :id",
                new MapSqlParameterSource().addValue("id", orderId).addValue("u", Timestamp.from(updatedAt)));
    }

    public void decrementItemCount(String orderId, Instant updatedAt) {
        jdbc.update("UPDATE order_summary SET item_count = GREATEST(0, item_count - 1), updated_at = :u WHERE order_id = :id",
                new MapSqlParameterSource().addValue("id", orderId).addValue("u", Timestamp.from(updatedAt)));
    }

    public void confirmOrder(String orderId, BigDecimal totalAmount, String workflowId, Instant confirmedAt) {
        jdbc.update("""
                UPDATE order_summary
                SET status = 'CONFIRMED', total_amount = :total, workflow_id = :wf,
                    confirmed_at = :confirmedAt, updated_at = :confirmedAt
                WHERE order_id = :id
                """,
                new MapSqlParameterSource()
                        .addValue("id", orderId)
                        .addValue("total", totalAmount)
                        .addValue("wf", workflowId)
                        .addValue("confirmedAt", Timestamp.from(confirmedAt)));
    }

    public void updateStatus(String orderId, String status, Instant updatedAt) {
        jdbc.update("UPDATE order_summary SET status = :status, updated_at = :u WHERE order_id = :id",
                new MapSqlParameterSource()
                        .addValue("id", orderId)
                        .addValue("status", status)
                        .addValue("u", Timestamp.from(updatedAt)));
    }

    public void completePayment(String orderId, Instant paidAt) {
        jdbc.update("""
                UPDATE order_summary
                SET status = 'PAYMENT_COMPLETED', payment_status = 'COMPLETED',
                    paid_at = :paidAt, updated_at = :paidAt
                WHERE order_id = :id
                """,
                new MapSqlParameterSource().addValue("id", orderId).addValue("paidAt", Timestamp.from(paidAt)));
    }

    public void failPayment(String orderId, Instant updatedAt) {
        jdbc.update("UPDATE order_summary SET payment_status = 'FAILED', updated_at = :u WHERE order_id = :id",
                new MapSqlParameterSource().addValue("id", orderId).addValue("u", Timestamp.from(updatedAt)));
    }

    public void refundPayment(String orderId, Instant updatedAt) {
        jdbc.update("UPDATE order_summary SET payment_status = 'REFUNDED', updated_at = :u WHERE order_id = :id",
                new MapSqlParameterSource().addValue("id", orderId).addValue("u", Timestamp.from(updatedAt)));
    }

    public void createShipment(String orderId, String trackingNumber, Instant updatedAt) {
        jdbc.update("""
                UPDATE order_summary
                SET status = 'SHIPPED', shipment_status = 'CREATED',
                    tracking_number = :tracking, updated_at = :u
                WHERE order_id = :id
                """,
                new MapSqlParameterSource()
                        .addValue("id", orderId)
                        .addValue("tracking", trackingNumber)
                        .addValue("u", Timestamp.from(updatedAt)));
    }

    public void deliverOrder(String orderId, Instant deliveredAt) {
        jdbc.update("""
                UPDATE order_summary
                SET status = 'DELIVERED', shipment_status = 'DELIVERED',
                    delivered_at = :deliveredAt, updated_at = :deliveredAt
                WHERE order_id = :id
                """,
                new MapSqlParameterSource().addValue("id", orderId).addValue("deliveredAt", Timestamp.from(deliveredAt)));
    }

    public void cancelOrder(String orderId, String cancelReason, Instant cancelledAt) {
        jdbc.update("""
                UPDATE order_summary
                SET status = 'CANCELLED', cancelled_at = :cancelledAt,
                    cancel_reason = :reason, updated_at = :cancelledAt
                WHERE order_id = :id
                """,
                new MapSqlParameterSource()
                        .addValue("id", orderId)
                        .addValue("reason", cancelReason)
                        .addValue("cancelledAt", Timestamp.from(cancelledAt)));
    }

    public Page<OrderSummaryResponse> findAll(List<String> statuses, String customerId, Pageable pageable) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        String where = buildWhere(statuses, customerId, params);

        params.addValue("limit", pageable.getPageSize());
        params.addValue("offset", pageable.getOffset());

        String selectSql = SELECT_COLS + " FROM order_summary" + where
                + " ORDER BY created_at DESC LIMIT :limit OFFSET :offset";
        String countSql = "SELECT COUNT(*) FROM order_summary" + where;

        List<OrderSummaryResponse> rows = jdbc.query(selectSql, params, this::mapRow);
        Long total = jdbc.queryForObject(countSql, params, Long.class);
        return new PageImpl<>(rows, pageable, total != null ? total : 0L);
    }

    private String buildWhere(List<String> statuses, String customerId, MapSqlParameterSource params) {
        StringBuilder where = new StringBuilder(" WHERE 1=1");
        if (statuses != null && !statuses.isEmpty()) {
            where.append(" AND status IN (:statuses)");
            params.addValue("statuses", statuses);
        }
        if (customerId != null) {
            where.append(" AND customer_id = :customerId");
            params.addValue("customerId", customerId);
        }
        return where.toString();
    }

    private OrderSummaryResponse mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new OrderSummaryResponse(
                rs.getString("order_id"),
                rs.getString("customer_id"),
                rs.getString("status"),
                rs.getString("payment_status"),
                rs.getString("shipment_status"),
                rs.getBigDecimal("total_amount"),
                rs.getInt("item_count"),
                rs.getString("shipping_address"),
                rs.getString("workflow_id"),
                rs.getString("tracking_number"),
                toInstant(rs.getTimestamp("created_at")),
                toInstant(rs.getTimestamp("confirmed_at")),
                toInstant(rs.getTimestamp("paid_at")),
                toInstant(rs.getTimestamp("delivered_at")),
                toInstant(rs.getTimestamp("cancelled_at")),
                rs.getString("cancel_reason"),
                toInstant(rs.getTimestamp("updated_at"))
        );
    }

    private static Instant toInstant(Timestamp ts) {
        return ts != null ? ts.toInstant() : null;
    }

    private static final String INSERT_SQL = """
            INSERT INTO order_summary (
                order_id, customer_id, status, payment_status, shipment_status,
                item_count, shipping_address, created_at, updated_at
            ) VALUES (
                :orderId, :customerId, 'DRAFT', 'PENDING', 'NOT_CREATED',
                0, :shippingAddress, :createdAt, :updatedAt
            )
            """;

    private static final String SELECT_COLS = """
            SELECT order_id, customer_id, status, payment_status, shipment_status,
                   total_amount, item_count, shipping_address, workflow_id, tracking_number,
                   created_at, confirmed_at, paid_at, delivered_at, cancelled_at,
                   cancel_reason, updated_at""";
}