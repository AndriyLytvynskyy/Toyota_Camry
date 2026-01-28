package com.ebay.challenge.streamprocessor.testutil;

import com.ebay.challenge.streamprocessor.engine.JoinEngine;
import com.ebay.challenge.streamprocessor.metrics.NoOpMetrics;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.output.OutputSink;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.EmittedPageViewStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;

import java.time.Instant;

public final class TestFactory {
    private TestFactory(){}

    public static JoinEngine createJoinEngine(OutputSink sink){
        return createJoinEngine(sink, 2);
    }

    public static JoinEngine createJoinEngine(OutputSink sink, int allowedLatenessMinutes){
        ClickStateStore clickStore = new ClickStateStore();
        EmittedPageViewStore pageViewStore = new EmittedPageViewStore();

        WatermarkTracker watermarkTracker = new WatermarkTracker(allowedLatenessMinutes);

        return new JoinEngine(
                clickStore,
                pageViewStore,
                watermarkTracker,
                sink,
                new NoOpMetrics()
        );
    }

    public static AdClickEvent click(String clickId, String user, Instant t, int partition) {
        AdClickEvent c = new AdClickEvent();
        c.setClickId(clickId);
        c.setUserId(user);
        c.setEventTime(t);
        c.setPartition(partition);
        return c;
    }

    public static PageViewEvent pageView(String id, String user, Instant t, int partition) {
        PageViewEvent pv = new PageViewEvent();
        pv.setEventId(id);
        pv.setUserId(user);
        pv.setEventTime(t);
        pv.setUrl("/page");
        pv.setPartition(partition);
        return pv;
    }
}
