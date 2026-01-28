package com.ebay.challenge.streamprocessor.metrics;

import com.ebay.challenge.streamprocessor.model.StreamType;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Component
@RequiredArgsConstructor
public class MetricsRegistry implements Metrics {

    private final WatermarkTracker watermarkTracker;

    private final AtomicLong clicksReceived = new AtomicLong();
    private final AtomicLong pageViewsReceived = new AtomicLong();
    private final AtomicLong pageViewsEmitted = new AtomicLong();
    private final AtomicLong pageViewsUpdated = new AtomicLong();

    private final AtomicLong clickStateSize = new AtomicLong();
    private final AtomicLong pageViewStateSize = new AtomicLong();

    private final AtomicReference<Instant> lastUpdatedAt =
            new AtomicReference<>(Instant.now());


    @Override
    public void onClickReceived() {
        clicksReceived.incrementAndGet();
        touch();
    }

    @Override
    public void onPageViewReceived() {
        pageViewsReceived.incrementAndGet();
        touch();
    }

    @Override
    public void onPageViewEmitted() {
        pageViewsEmitted.incrementAndGet();
        touch();
    }

    @Override
    public void onPageViewUpdated(int updates) {
        if (updates <= 0) {
            return;
        }
        pageViewsUpdated.addAndGet(updates);
        touch();
    }

    @Override
    public void onClickStateSizeUpdated(long size) {
        clickStateSize.set(size);
        touch();
    }

    @Override
    public void onPageViewStateSizeUpdated(long size) {
        pageViewStateSize.set(size);
        touch();
    }

    /* ---------- Snapshot ---------- */

    @Override
    public MetricsSnapshot snapshot() {

        List<JoinPartitionWatermark> joinWatermarks =
                buildJoinWatermarks()
                        .values()
                        .stream()
                        .sorted(Comparator.comparingInt(JoinPartitionWatermark::partition))
                        .toList();

        return new MetricsSnapshot(
                clicksReceived.get(),
                pageViewsReceived.get(),
                pageViewsEmitted.get(),
                pageViewsUpdated.get(),
                clickStateSize.get(),
                pageViewStateSize.get(),
                joinWatermarks,
                lastUpdatedAt.get()
        );
    }


    /**
     * This method is just used for UI reports
     *
     * @return Map with partition number and then DTO JoinPartitionWatermark to show on UI
     */
    private Map<Integer, JoinPartitionWatermark> buildJoinWatermarks() {

        Map<Integer, JoinPartitionWatermark> result = new HashMap<>();

        watermarkTracker.getPartitionMaxEventTimeSeen()
                .forEach((logicalPartitionId, maxEventTime) -> {

                    int idx = logicalPartitionId.lastIndexOf('_');
                    if (idx < 0) {
                        return;
                    }

                    String stream = logicalPartitionId.substring(0, idx);
                    int partition = Integer.parseInt(
                            logicalPartitionId.substring(idx + 1)
                    );

                    result.compute(partition, (p, existing) -> {

                        Instant pvMax =
                                existing != null ? existing.pageViewsMaxEventTime() : null;
                        Instant clkMax =
                                existing != null ? existing.adClicksMaxEventTime() : null;

                        if (stream.equals(StreamType.PAGE_VIEWS.topicName)) {
                            pvMax = maxEventTime;
                        } else if (stream.equals(StreamType.AD_CLICKS.topicName)) {
                            clkMax = maxEventTime;
                        }

                        Instant joinWm = watermarkTracker.getWatermark(partition);
                        if (joinWm.equals(Instant.MIN)) {
                            joinWm = null;
                        }

                        return new JoinPartitionWatermark(
                                partition,
                                pvMax,
                                clkMax,
                                joinWm
                        );
                    });
                });

        return result;
    }


    private void touch() {
        lastUpdatedAt.set(Instant.now());
    }
}
