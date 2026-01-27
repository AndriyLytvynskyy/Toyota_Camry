package com.ebay.challenge.streamprocessor.model;

/**
 * This holds different StreamTypes, so that its easier to work with them in watermarking
 */
public enum StreamType {
    AD_CLICKS("ad_clicks"),
    PAGE_VIEWS("page_views");

    private final String topicName;

    StreamType(String topicName){
        this.topicName = topicName;
    }

    public String logicalPartition(int partition){
        return topicName + "_" + partition;
    }

}
