package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.model.StreamType;
import com.ebay.challenge.streamprocessor.output.OutputSink;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.EmittedPageViewStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Core join engine implementing 'emit immediately':
 * emit immediately, update later if needed.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JoinEngine {

    private final ClickStateStore clickStore;
    private final EmittedPageViewStore emittedPageViewStore;
    private final WatermarkTracker watermarkTracker;
    private final OutputSink outputSink;


    /**
     * Track partitions observed by this instance.
     * Used for periodic eviction.
     */
    private final Set<Integer> knownPartitions =
            ConcurrentHashMap.newKeySet();


    /**
     * Process an ad click event.
     * - Update watermark
     * - Drop if too late
     * - Store click
     * - Try updating already emitted page views
     */
    public void processClick(AdClickEvent click) {
        int partition = click.getPartition();
        Instant eventTime = click.getEventTime();
        knownPartitions.add(partition);
        // Update watermark for click stream
        watermarkTracker.updateWatermark(StreamType.AD_CLICKS, partition,
                eventTime
        );

        if (watermarkTracker.isTooLate(partition, eventTime)) {
            log.warn(
                    "Dropping late ad click {} (partition={}, eventTime={})",
                    click.getClickId(), partition, eventTime
            );
            return;
        }

        // Store click
        clickStore.addClick(click);

        // Try updating emitted page views
        Instant watermark = watermarkTracker.getWatermark(partition);
        emittedPageViewStore.tryUpdateWithClick(
                click,
                watermark,
                outputSink::write
        );
    }

    /**
     * Process a page view event.
     *
     * Semantics (Option B):
     * - Update watermark
     * - Drop if too late
     * - Emit immediately
     * - Record for possible future updates
     */
    public void processPageView(PageViewEvent pageView) {
        int partition = pageView.getPartition();
        Instant eventTime = pageView.getEventTime();

        knownPartitions.add(partition);
        // Update watermark for page view stream
        watermarkTracker.updateWatermark(
                StreamType.PAGE_VIEWS, partition,
                eventTime
        );

        if (watermarkTracker.isTooLate(partition, eventTime)) {
            log.warn(
                    "Dropping late page view {} (partition={}, eventTime={})",
                    pageView.getEventId(), partition, eventTime
            );
            return;
        }

        // Find best click so far
        AdClickEvent click =
                clickStore.findAttributableClick(
                        pageView.getUserId(),
                        pageView.getEventTime()
                );

        // Emit immediately
        AttributedPageView output =
                buildAttributedPageView(pageView, click);

        outputSink.write(output);

        // Record for possible updates
        emittedPageViewStore.recordEmittedPageView(pageView, click);

        log.info(
                "Emitted attributed page view {} immediately (user={}, click={})",
                pageView.getEventId(),
                pageView.getUserId(),
                click != null ? click.getClickId() : "none"
        );
    }


    @Scheduled(fixedRate = 30000)
    public void evictFinalizedState() {
        for (Integer partition : knownPartitions) {
            Instant watermark = watermarkTracker.getWatermark(partition);
            if (watermark.equals(Instant.MIN)) {
                continue;
            }

            int pvEvicted = emittedPageViewStore.evictFinalizedPageViews(watermark);

            Instant clickCutoff =
                    watermark.minus(ClickStateStore.ATTRIBUTION_WINDOW);

            int clicksEvicted =
                    clickStore.evictOldClicks(clickCutoff);

            if (pvEvicted > 0 || clicksEvicted > 0) {
                log.debug(
                        "Eviction of partition {}: pageViews={}, clicks={}",
                        partition, pvEvicted, clicksEvicted
                );
            }
        }
    }

    private AttributedPageView buildAttributedPageView(
            PageViewEvent pageView,
            AdClickEvent click
    ) {
        return AttributedPageView.builder()
                .pageViewId(pageView.getEventId())
                .userId(pageView.getUserId())
                .eventTime(pageView.getEventTime())
                .url(pageView.getUrl())
                .attributedCampaignId(
                        click != null ? click.getCampaignId() : null
                )
                .attributedClickId(
                        click != null ? click.getClickId() : null
                )
                .build();
    }
}
