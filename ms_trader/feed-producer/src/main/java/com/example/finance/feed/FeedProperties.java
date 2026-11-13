package com.example.finance.feed;

import java.nio.file.Path;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("feed")
public record FeedProperties(
    Path directory,
    int chunkSize,
    long maxBadRecords,
    double maxBadPercent,
    int maxRecordBytes,
    String policyId) {
    public FeedProperties {
        if (directory == null
            || chunkSize < 1
            || chunkSize > 10000
            || maxBadRecords < 0
            || !Double.isFinite(maxBadPercent)
            || maxBadPercent < 0
            || maxBadPercent > 100
            || maxRecordBytes < 128
            || maxRecordBytes > 1048576
            || policyId == null
            || !policyId.matches("[A-Za-z0-9_-]{1,32}"))
            throw new IllegalArgumentException("Invalid feed configuration");
        directory = directory.toAbsolutePath().normalize();
    }
}
