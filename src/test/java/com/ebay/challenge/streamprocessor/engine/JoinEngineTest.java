package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.output.InMemoryOutputSink;
import com.ebay.challenge.streamprocessor.testutil.TestFactory;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static com.ebay.challenge.streamprocessor.testutil.TestFactory.click;
import static com.ebay.challenge.streamprocessor.testutil.TestFactory.pageView;
import static org.assertj.core.api.Assertions.assertThat;

public class JoinEngineTest {

    @Test
    void testClickBeforePageView() {
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = TestFactory.createJoinEngine(sink, 5);

        Instant base = Instant.parse("2026-01-24T12:00:00Z");

        engine.processClick(click("click1", "u1", base.plusSeconds(300), 0));
        engine.processPageView(pageView("pv1", "u1", base.plusSeconds(600), 0));

        assertThat(sink.records()).hasSize(1);
        assertThat(sink.records().getFirst().getAttributedClickId()).isEqualTo("click1");
    }

    @Test
    void testMultipleClicksPickLatest() {
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = TestFactory.createJoinEngine(sink, 5);

        Instant base = Instant.parse("2026-01-24T12:00:00Z");

        engine.processClick(click("click1", "u1", base.plusSeconds(300), 0));
        engine.processClick(click("click2", "u1", base.plusSeconds(600), 0));
        engine.processPageView(pageView("pv1", "u1", base.plusSeconds(900), 0));

        assertThat(sink.records()).hasSize(1);
        assertThat(sink.records().getFirst().getAttributedClickId()).isEqualTo("click2");
    }

    @Test
    void testClickOutsideWindowNotAttributed() {
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = TestFactory.createJoinEngine(sink, 5);

        Instant base = Instant.parse("2024-01-01T12:00:00Z");

        engine.processClick(click("click1", "u1", base, 0));
        engine.processPageView(pageView("pv1", "u1", base.plusSeconds(2100), 0));

        assertThat(sink.records()).hasSize(1);
        assertThat(sink.records().getFirst().getAttributedClickId()).isNull();
    }

    @Test
    void testOutOfOrderClickUpdatesPageView() {
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = TestFactory.createJoinEngine(sink, 5);

        Instant base = Instant.parse("2026-01-24T12:00:00Z");

        engine.processPageView(pageView("pv1", "u1", base.plusSeconds(600), 0));
        engine.processClick(click("click1", "u1", base.plusSeconds(300), 0));

        assertThat(sink.records()).hasSize(2);

        AttributedPageView updated = sink.records().get(1);
        assertThat(updated.getAttributedClickId()).isEqualTo("click1");
    }

    @Test
    void testVeryLateClickDropped() {
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = TestFactory.createJoinEngine(sink, 5);

        Instant base = Instant.parse("2026-01-24T12:00:00Z");

        engine.processClick(click("new", "u1", base.plusSeconds(600), 0));
        engine.processPageView(pageView("pv", "u1", base.plusSeconds(601), 0));

        // watermark now has moved further
        engine.processClick(click("late", "u1", base.plusSeconds(300), 0));

        assertThat(sink.records()).hasSize(1);
        assertThat(sink.records().getFirst().getAttributedClickId()).isEqualTo("new");
    }

    @Test
    void testPageViewWithNoClick() {
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = TestFactory.createJoinEngine(sink, 5);

        engine.processPageView(
                pageView("pv1", "no-click-user",
                        Instant.parse("2026-01-24T12:00:00Z"), 0)
        );

        assertThat(sink.records()).hasSize(1);
        assertThat(sink.records().getFirst().getAttributedClickId()).isNull();
    }
}
