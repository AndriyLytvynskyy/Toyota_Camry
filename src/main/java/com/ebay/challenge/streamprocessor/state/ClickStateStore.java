package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stores ad click events partitioned by user_id for efficient windowed joins.
 * <p>
 * Thread-safe implementation with per-user locking for fine-grained concurrency.
 * Implements state eviction to prevent unbounded memory growth.
 * <p>
 * TODO: Implement thread-safe state storage and retrieval
 */
@Slf4j
@Component
public class ClickStateStore {

    // Attribution window: clicks within last 30 minutes can be attributed
    public static final Duration ATTRIBUTION_WINDOW = Duration.ofMinutes(30);

    // Hint: Consider using ConcurrentHashMap and TreeSet for thread-safe, sorted storage
    private final ConcurrentMap<String, TreeSet<AdClickEvent>> clicksPerUser = new ConcurrentHashMap<>();

    private final AtomicLong totalClicks = new AtomicLong(0);

    /**
     * We guarantee deterministic ordering here:
     * <p>
     * Here we ensure that clicks with same timestamp are not accidentally
     * dropped by the TreeSet
     */
    private static final Comparator<AdClickEvent> CLICKS_ORDER_MOST_RECENT_FIRST = Comparator
            .comparing(AdClickEvent::getEventTime, Comparator.reverseOrder())
            .thenComparing(AdClickEvent::getClickId);

    /**
     * Add a click event to the state store.
     * <p>
     * - Use locks for thread safety
     * - Store clicks sorted by event time (most recent first)
     * - Handle concurrent access properly
     * <p>
     * Here we need to synchronize on TreeSet as ConcurrentHashMap makes access to the map thread-safe, not access to
     * the objects stored inside it.
     * Multiple threads can safely:
     * add/remove entries
     * call compute, get, put
     *
     * @param click the ad click event
     */
    public void addClick(AdClickEvent click) {
        log.debug("Adding click {} for user {}", click.getClickId(), click.getUserId());
        clicksPerUser.compute(click.getUserId(), (userId, set) -> {
            if (set == null) {
                set = new TreeSet<>(CLICKS_ORDER_MOST_RECENT_FIRST);
            }

            // we make sure that all TreeSet mutations are synchronized
            // safe under concurrent listeners
            synchronized (set) {
                boolean added = set.add(click);
                if (added) {
                    totalClicks.incrementAndGet();
                }
            }
            return set;
        });
    }

    /**
     * Helper predicate which checks if click event is in specific window
     *
     * @param click        AdClickEvent
     * @param pageViewTime Time of page view
     * @return True if in range of windowStart - pageViewTime
     */
    private boolean isClickWithinAttributionWindow(
            AdClickEvent click,
            Instant pageViewTime
    ) {
        Instant windowStart = pageViewTime.minus(ATTRIBUTION_WINDOW);
        Instant t = click.getEventTime();
        return !t.isAfter(pageViewTime) && !t.isBefore(windowStart);
    }

    /**
     * Find the most recent click for a user within the attribution window.
     * <p>
     * - Search for clicks in window: [pageViewTime - 30 minutes, pageViewTime]
     * - Return the most recent click within the window
     * - Return null if no click found
     *
     * @param userId       the user ID
     * @param pageViewTime the page view event time
     * @return the most recent click within 30 minutes before the page view, or null if none found
     */
    public AdClickEvent findAttributableClick(String userId, Instant pageViewTime) {
        log.debug("Finding attributable click for user {} at time {}", userId, pageViewTime);
        TreeSet<AdClickEvent> clicks = clicksPerUser.get(userId);
        if (clicks == null || clicks.isEmpty()) {
            return null;
        }
        synchronized (clicks) {
            return clicks.stream()
                    .filter(c -> isClickWithinAttributionWindow(c, pageViewTime)
                    ).findFirst().orElse(null);
        }
    }

    /**
     * Evict old clicks that are beyond the retention window.
     * Prevents unbounded memory growth.
     * <p>
     * - Remove clicks older than the cutoff time
     * - Clean up empty user entries
     * - Return count of evicted clicks
     *
     * @param cutoffTime clicks older than this time should be evicted
     * @return number of clicks evicted
     */
    public int evictOldClicks(Instant cutoffTime) {

        int evicted = 0;

        for (var entry : clicksPerUser.entrySet()) {
            TreeSet<AdClickEvent> set = entry.getValue();
            synchronized (set) {
                Iterator<AdClickEvent> it = set.iterator();
                while (it.hasNext()) {
                    AdClickEvent click = it.next();
                    if (click.getEventTime().isBefore(cutoffTime)) {
                        it.remove();
                        evicted++;
                        totalClicks.decrementAndGet();
                    }
                    if (set.isEmpty()) {
                        clicksPerUser.remove(entry.getKey(), set);
                    }
                }
            }
        }
        if (evicted > 0) {
            log.debug("Evicted {} old clicks", evicted);
        }
        return evicted;
    }

    /**
     * Get the total number of clicks currently in state.
     *
     * @return total click count across all users
     */
    public long getTotalClickCount() {
        return totalClicks.get();
    }
}
