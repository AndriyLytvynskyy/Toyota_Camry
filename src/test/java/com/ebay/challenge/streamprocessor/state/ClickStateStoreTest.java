package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

public class ClickStateStoreTest {
    private AdClickEvent click(
            String clickId,
            String userId,
            Instant eventTime
    ) {
        AdClickEvent click = new AdClickEvent();
        click.setClickId(clickId);
        click.setUserId(userId);
        click.setEventTime(eventTime);
        return click;
    }

    /**
     * Should be able to add a click and retrieve it for attribution.
     */
    @Test
    void testAddAndRetrieveClick() {
        ClickStateStore store = new ClickStateStore();

        Instant clickTime = Instant.parse("2026-01-24T12:00:00Z");
        AdClickEvent click = click("click1", "user1", clickTime);

        store.addClick(click);

        AdClickEvent result =
                store.findAttributableClick("user1", clickTime.plusSeconds(60));

        assertThat(result).isNotNull();
        assertThat(result.getClickId()).isEqualTo("click1");
    }

    /**
     * Add clicks at different times
     * Evict with watermark
     * Verify old clicks are gone
     */
    @Test
    public void testEvictionRemovesOldClicks() {
        ClickStateStore store = new ClickStateStore();

        Instant now = Instant.parse("2026-01-24T12:30:00Z");

        // Old click (outside attribution window)
        AdClickEvent oldClick =
                click("old", "user1", now.minusSeconds(3600)); // 1 hour ago

        // Recent click (inside attribution window)
        AdClickEvent recentClick =
                click("recent", "user1", now.minusSeconds(300)); // 5 min ago

        store.addClick(oldClick);
        store.addClick(recentClick);

        Instant cutoffTime = now.minusSeconds(1800); // 30 min
        int evictedCount = store.evictOldClicks(cutoffTime);

        assertThat(evictedCount).isEqualTo(1);

        // Verify there is no old click
        AdClickEvent result =
                store.findAttributableClick("user1", now);

        assertThat(result).isNotNull();
        assertThat(result.getClickId()).isEqualTo("recent");
    }

    /**
     * Should return latest click within window.
     *
     * Add multiple clicks for same user
     * Query with page view time
     * Should get latest click in window
     */
    @Test
    public void testLatestClickInWindow(){
        ClickStateStore store = new ClickStateStore();

        Instant pageViewTime = Instant.parse("2026-01-24T12:00:00Z");

        // Clicks within the 30-minute attribution window
        AdClickEvent click1 =
                click("click1", "user1", pageViewTime.minusSeconds(1500)); // 25 min ago

        AdClickEvent click2 =
                click("click2", "user1", pageViewTime.minusSeconds(600)); // 10 min ago

        AdClickEvent click3 =
                click("click3", "user1", pageViewTime.minusSeconds(300)); // 5 min ago

        store.addClick(click1);
        store.addClick(click2);
        store.addClick(click3);

        AdClickEvent result =
                store.findAttributableClick("user1", pageViewTime);

        assertThat(result).isNotNull();
        assertThat(result.getClickId()).isEqualTo("click3");
    }
}
