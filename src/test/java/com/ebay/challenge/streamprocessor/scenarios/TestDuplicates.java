package com.ebay.challenge.streamprocessor.scenarios;

import com.ebay.challenge.streamprocessor.engine.JoinEngine;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.output.InMemoryOutputSink;
import com.ebay.challenge.streamprocessor.testutil.TestFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static com.ebay.challenge.streamprocessor.testutil.TestFactory.click;
import static com.ebay.challenge.streamprocessor.testutil.TestFactory.pageView;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestDuplicates {

    @Test
    public void testDuplicateClicksDoNotCauseDuplicatedOutput(){
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = TestFactory.createJoinEngine(sink, 15);

        Instant base = Instant.parse("2024-01-01T12:00:00Z");

        engine.processPageView(pageView("pv1", "user_1", base, 0));

        Assertions.assertEquals(1, sink.records().size(), "Initial page view emitted");
        assertNull(
                sink.records().getFirst().getAttributedClickId()
        );

        // First click arrives → update expected
        AdClickEvent click1 = click("click_1", "user_1", base.minusSeconds(60), 0);
        engine.processClick(click1);

        Assertions.assertEquals(2, sink.records().size(), "One update should be emitted");
        Assertions.assertEquals("click_1", sink.records().get(1).getAttributedClickId());

        //that same click arrives once again - we don't update
        engine.processClick(click1);

        // Assert: NO additional update
        Assertions.assertEquals(2, sink.records().size(), "Duplicate click must not cause another update");
    }
}
