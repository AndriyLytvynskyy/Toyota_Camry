package com.ebay.challenge.streamprocessor.metrics;

import com.ebay.challenge.streamprocessor.model.StreamType;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * MetricsRegistry which holds metrics for UI
 */
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


    @Override
    public MetricsSnapshot snapshot() {

        return new MetricsSnapshot(
                clicksReceived.get(),
                pageViewsReceived.get(),
                pageViewsEmitted.get(),
                pageViewsUpdated.get(),
                clickStateSize.get(),
                pageViewStateSize.get(),
                buildJoinWatermarks(),
                lastUpdatedAt.get()
        );
    }


    /**
     * This method is just used for UI reports
     *
     * @return List with partition number and then DTO JoinPartitionWatermark to show on UI
     */
    private List<JoinPartitionWatermark> buildJoinWatermarks() {

        Map<Integer, Instant> pvMaxByPartition = new HashMap<>();
        Map<Integer, Instant> clickMaxByPartition = new HashMap<>();

        watermarkTracker.getPartitionMaxEventTimeSeen()
                .forEach((logicalPartitionId, maxEventTime) -> {
                    TopicPartition topicPart = getTopicPartition(logicalPartitionId);

                    if (topicPart.topic().equals(StreamType.PAGE_VIEWS.topicName)) {
                        pvMaxByPartition.put(topicPart.partition(), maxEventTime);
                    } else if (topicPart.topic().equals(StreamType.AD_CLICKS.topicName)) {
                        clickMaxByPartition.put(topicPart.partition(), maxEventTime);
                    }
                });

        return watermarkTracker.getActivePartitions()
                .stream()
                .sorted()
                .map(partition -> {
                    Instant joinWm = watermarkTracker.getWatermark(partition);
                    if (joinWm.equals(Instant.MIN)) {
                        joinWm = null;
                    }

                    return new JoinPartitionWatermark(
                            partition,
                            pvMaxByPartition.get(partition),
                            clickMaxByPartition.get(partition),
                            joinWm
                    );
                })
                .toList();
    }

    private static TopicPartition getTopicPartition(String logicalPartitionId) {
        int idx = logicalPartitionId.lastIndexOf('_');
        String topic = logicalPartitionId.substring(0, idx);
        int partition = Integer.parseInt(
                logicalPartitionId.substring(idx + 1)
        );
        return new TopicPartition(topic, partition);
    }

    private record TopicPartition(String topic, int partition) {
    }


    private void touch() {
        lastUpdatedAt.set(Instant.now());
    }
}
