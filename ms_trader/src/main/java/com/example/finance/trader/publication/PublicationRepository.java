package com.example.finance.trader.publication;

import com.example.finance.trader.application.EventMetadata;

import java.time.Instant;
import java.util.Optional;

public interface PublicationRepository {
    Optional<PublicationTask> claimNext(Instant now, Instant leaseUntil, String token);

    boolean markPublished(PublicationTask task, EventMetadata event);

    void retryLater(PublicationTask task, Instant nextAttemptAt);
}
