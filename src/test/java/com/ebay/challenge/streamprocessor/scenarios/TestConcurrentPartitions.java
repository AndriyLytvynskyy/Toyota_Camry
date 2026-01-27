package com.ebay.challenge.streamprocessor.scenarios;

import com.ebay.challenge.streamprocessor.engine.JoinEngine;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.output.InMemoryOutputSink;
import com.ebay.challenge.streamprocessor.testutil.TestFactory;

import java.time.Instant;
import org.junit.jupiter.api.Test;

import static com.ebay.challenge.streamprocessor.testutil.TestFactory.click;
import static com.ebay.challenge.streamprocessor.testutil.TestFactory.pageView;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConcurrentPartitions {

    @Test
    void testConcurrentPartitionsWithThreads() throws Exception {
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = TestFactory.createJoinEngine(sink, 5);

        Instant base = Instant.parse("2026-01-24T12:00:00Z");

        Runnable p0 = () -> {
            engine.processClick(click("click1_p0", "u1", base.plusSeconds(300), 0));
            engine.processPageView(pageView("pv1_p0", "u1", base.plusSeconds(600), 0));
        };

        Runnable p1 = () -> {
            engine.processClick(click("click1_p1", "u2", base.plusSeconds(400), 1));
            engine.processPageView(pageView("pv1_p1", "u2", base.plusSeconds(500), 1));
        };

        Thread t1 = new Thread(p0);
        Thread t2 = new Thread(p1);

        t1.start();
        t2.start();
        t1.join();
        t2.join();

        assertThat(sink.records()).hasSize(2);
    }

    @Test
    void testConcurrentPartitionsIsolation(){
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = TestFactory.createJoinEngine(sink, 5);

        Instant base = Instant.parse("2026-01-24T12:00:00Z");

        // Partition 0 events (user_1)
        engine.processClick(
                click("click1_p0", "user_1", base.plusSeconds(300), 0)
        );

        // Partition 1 events (user_2)
        engine.processClick(
                click("click1_p1", "user_2", base.plusSeconds(400), 1)
        );

        // Interleave page views
        engine.processPageView(
                pageView("pv1_p1", "user_2", base.plusSeconds(500), 1)
        );

        engine.processPageView(
                pageView("pv1_p0", "user_1", base.plusSeconds(600), 0)
        );

        // We should get exactly 2 outputs
        assertThat(sink.records()).hasSize(2);

        // Verify partition 1 attribution
        AttributedPageView pvUser2 =
                sink.records().stream()
                        .filter(pv -> pv.getPageViewId().equals("pv1_p1"))
                        .findFirst()
                        .orElseThrow();

        assertThat(pvUser2.getUserId()).isEqualTo("user_2");
        assertThat(pvUser2.getAttributedClickId()).isEqualTo("click1_p1");

        // Verify partition 0 attribution
        AttributedPageView pvUser1 =
                sink.records().stream()
                        .filter(pv -> pv.getPageViewId().equals("pv1_p0"))
                        .findFirst()
                        .orElseThrow();

        assertThat(pvUser1.getUserId()).isEqualTo("user_1");
        assertThat(pvUser1.getAttributedClickId()).isEqualTo("click1_p0");
    }
}
