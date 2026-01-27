package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Stores already-emitted page views for Option B (emit immediately, update later).
 *
 * A page view can be updated by late clicks until it is finalized by the watermark.
 */
@Slf4j
@Component
public class EmittedPageViewStore {

    /**
     * page_view_id -> PageViewState
     */
    private final ConcurrentHashMap<String, PageViewState> state =
            new ConcurrentHashMap<>();

    /**
     * Record a newly emitted page view.
     */
    public void recordEmittedPageView(PageViewEvent pageView, AdClickEvent click) {
        state.put(
                pageView.getEventId(),
                new PageViewState(
                        pageView,
                        click != null ? click.getEventTime() : null
                )
        );
    }

    /**
     * Try to update previously emitted page views with a late-arriving click.
     *
     * Update is applied only if:
     * - same user
     * - click is before page view
     * - click is within attribution window
     * - page view is not finalized by watermark
     * - click improves attribution
     */
    public void tryUpdateWithClick(
            AdClickEvent click,
            Instant watermark,
            Consumer<AttributedPageView> onUpdate
    ) {
        for (PageViewState pageViewState : state.values()) {
            PageViewEvent pv = pageViewState.pageView;

            // Must match user
            if (!pv.getUserId().equals(click.getUserId())) {
                continue;
            }

            // Stop updates after finalization
            if (!watermark.equals(Instant.MIN)
                    && pv.getEventTime().isBefore(watermark)) {
                continue;
            }

            // Click must be before page view
            if (click.getEventTime().isAfter(pv.getEventTime())) {
                continue;
            }

            // Click must be within attribution window
            if (click.getEventTime().isBefore(
                    pv.getEventTime().minus(ClickStateStore.ATTRIBUTION_WINDOW)
            )) {
                continue;
            }

            synchronized (pageViewState) {
                // Update only if better click
                if (pageViewState.attributedClickTime == null
                        || click.getEventTime().isAfter(pageViewState.attributedClickTime)) {

                    AttributedPageView updated =
                            AttributedPageView.builder()
                                    .pageViewId(pv.getEventId())
                                    .userId(pv.getUserId())
                                    .eventTime(pv.getEventTime())
                                    .url(pv.getUrl())
                                    .attributedCampaignId(click.getCampaignId())
                                    .attributedClickId(click.getClickId())
                                    .build();

                    pageViewState.attributedClickTime = click.getEventTime();
                    onUpdate.accept(updated);

                    log.info(
                            "Updated page view {} with late click {}",
                            pv.getEventId(), click.getClickId()
                    );
                }
            }
        }
    }

    /**
     * Evict finalized page views.
     *
     * A page view is finalized when:
     *   watermark >= pageView.eventTime
     */
    public int evictFinalizedPageViews(Instant watermark) {
        if (watermark.equals(Instant.MIN)) {
            return 0;
        }

        int before = state.size();

        state.entrySet().removeIf(entry ->
                entry.getValue().pageView.getEventTime().isBefore(watermark)
        );

        int evicted = before - state.size();
        if (evicted > 0) {
            log.debug("Evicted {} finalized page views", evicted);
        }

        return evicted;
    }

    public int size() {
        return state.size();
    }

    /**
     * Internal per-page-view state.
     */
    private static final class PageViewState {
        final PageViewEvent pageView;
        volatile Instant attributedClickTime;

        PageViewState(PageViewEvent pageView, Instant attributedClickTime) {
            this.pageView = pageView;
            this.attributedClickTime = attributedClickTime;
        }
    }
}
