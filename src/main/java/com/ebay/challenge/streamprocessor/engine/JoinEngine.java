package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.metrics.Metrics;
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

/**
 * Core join engine implementing `emit immediately, update later if needed`.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JoinEngine {

    private final ClickStateStore clickStore;
    private final EmittedPageViewStore emittedPageViewStore;
    private final WatermarkTracker watermarkTracker;
    private final OutputSink outputSink;
    private final Metrics metrics;


    /**
     * Process an ad click event.
     * - Update watermark
     * - Drop if too late
     * - Store click
     * - Try updating already emitted page views
     */
    public void processClick(AdClickEvent click) {
        metrics.onClickReceived();

        int partition = click.getPartition();
        Instant eventTime = click.getEventTime();
        watermarkTracker.updateWatermark(StreamType.AD_CLICKS, partition,
                eventTime
        );
        if (watermarkTracker.isTooLate(partition, eventTime)) {
            log.warn(
                    "Dropping late ad click {} (partition={}, eventTime={})",
                    click.getClickId(), StreamType.AD_CLICKS.logicalPartition(partition), eventTime
            );
            return;
        }
        clickStore.addClick(click);
        metrics.onClickStateSizeUpdated(clickStore.getTotalClickCount());

        Instant joinWatermark = watermarkTracker.getWatermark(partition);
        int pvUpdates = emittedPageViewStore.tryUpdateWithClick(
                click,
                joinWatermark,
                outputSink::write
        );
        if (pvUpdates > 0) {
            metrics.onPageViewUpdated(pvUpdates);
        }
    }


    /**
     * Process a page view event.
     *
     * - Update watermark
     * - Drop if too late
     * - Emit immediately
     * - Record for possible future updates
     */
    public void processPageView(PageViewEvent pageView) {
        metrics.onPageViewReceived();
        int partition = pageView.getPartition();

        Instant pvEventTime = pageView.getEventTime();
        watermarkTracker.updateWatermark(
                StreamType.PAGE_VIEWS, partition,
                pvEventTime
        );

        if (watermarkTracker.isTooLate(partition, pvEventTime)) {
            log.warn(
                    "Dropping late page view {} (partition={}, eventTime={})",
                    pageView.getEventId(), StreamType.PAGE_VIEWS.logicalPartition(partition), pvEventTime
            );
            return;
        }
        AdClickEvent click =
                clickStore.findAttributableClick(
                        pageView.getUserId(),
                        pageView.getEventTime()
                );

        AttributedPageView attributedPageView =
                buildAttributedPageView(pageView, click);
        outputSink.write(attributedPageView);
        metrics.onPageViewEmitted();

        emittedPageViewStore.recordEmittedPageView(pageView, click);
        metrics.onPageViewStateSizeUpdated(emittedPageViewStore.size());

        log.info(
                "Emitted attributed page view {} immediately (user={}, click={})",
                pageView.getEventId(),
                pageView.getUserId(),
                click != null ? click.getClickId() : "none"
        );
    }


    @Scheduled(fixedRate = 30000)
    public void evictFinalizedState() {
        for (Integer partition : watermarkTracker.getActivePartitions()) {
            Instant watermark = watermarkTracker.getWatermark(partition);
            if (watermark.equals(Instant.MIN)) {
                continue;
            }
            Instant clickCutoff =
                    watermark.minus(ClickStateStore.ATTRIBUTION_WINDOW);

            int clicksEvicted =
                    clickStore.evictOldClicks(clickCutoff);
            int pvEvicted = emittedPageViewStore.evictFinalizedPageViews(watermark);
            if (pvEvicted > 0 || clicksEvicted > 0) {
                log.debug(
                        "Eviction of partition {}: pageViews={}, clicks={}",
                        partition, pvEvicted, clicksEvicted
                );
                metrics.onPageViewStateSizeUpdated(emittedPageViewStore.size());
                metrics.onClickStateSizeUpdated(clickStore.getTotalClickCount());
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
