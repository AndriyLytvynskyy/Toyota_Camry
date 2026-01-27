package com.ebay.challenge.streamprocessor.output;

import com.ebay.challenge.streamprocessor.model.AttributedPageView;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class InMemoryOutputSink extends OutputSink {

    private final List<AttributedPageView> outputs = new CopyOnWriteArrayList<>();

    @Override
    public void write(AttributedPageView attributedPageView) {
        outputs.add(attributedPageView);
    }

    public List<AttributedPageView> records() {
        return outputs;
    }
}

