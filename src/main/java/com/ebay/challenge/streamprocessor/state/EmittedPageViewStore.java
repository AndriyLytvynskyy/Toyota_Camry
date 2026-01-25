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
 * We need this class to store already emitted page views,
 * in case of late click event - we could update existing PageView
 *
 */
@Slf4j
@Component
public class EmittedPageViewStore {

    /**
     * Stores already-emitted page views for possible attribution updates.
     * Keyed by page_view_id.
     */
    private final ConcurrentHashMap<String, PageViewState> state =
            new ConcurrentHashMap<>();

    /**
     * Record a newly emitted page view.
     * Called when a page view is emitted for the first time.
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
     * If the click improves attribution, an updated AttributedPageView is emitted
     * via the provided callback.
     */
    public void tryUpdateWithClick(
            AdClickEvent click,
            Consumer<AttributedPageView> onUpdate
    ) {
        for (PageViewState s : state.values()) {
            PageViewEvent pv = s.pageView;


            if (!pv.getUserId().equals(click.getUserId())) {
                continue;
            }

            // Click must be before page view (event time)
            if (click.getEventTime().isAfter(pv.getEventTime())) {
                continue;
            }

            synchronized (s) {
                // Only update if this click is better (more recent)
                if (s.attributedClickTime == null ||
                        click.getEventTime().isAfter(s.attributedClickTime)) {

                    AttributedPageView updated =
                            AttributedPageView.builder()
                                    .pageViewId(pv.getEventId())
                                    .userId(pv.getUserId())
                                    .eventTime(pv.getEventTime())
                                    .url(pv.getUrl())
                                    .attributedCampaignId(click.getCampaignId())
                                    .attributedClickId(click.getClickId())
                                    .build();

                    s.attributedClickTime = click.getEventTime();
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
     * Evict page views whose attribution is finalized.
     * Safe when watermark >= page_view_time + attribution_window.
     *
     * @return number of evicted page views
     */
    public int evictFinalized(Instant watermark) {
        int before = state.size();

        state.entrySet().removeIf(entry ->
                watermark.isAfter(
                        entry.getValue().pageView.getEventTime()
                                .plus(ClickStateStore.ATTRIBUTION_WINDOW)
                )
        );

        int evicted = before - state.size();
        if (evicted > 0) {
            log.debug("Evicted {} finalized page views", evicted);
        }
        return evicted;
    }

    /**
     * Visible for testing / metrics.
     */
    public int size() {
        return state.size();
    }


    /**
     * Page View State
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
