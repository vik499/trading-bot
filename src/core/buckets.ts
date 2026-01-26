export function bucketCloseTs(exchangeTs: number, bucketMs: number): number {
    const safeBucketMs = Math.max(1, bucketMs);
    const start = Math.floor(exchangeTs / safeBucketMs) * safeBucketMs;
    return start + safeBucketMs;
}

export function bucketStartTs(exchangeTs: number, bucketMs: number): number {
    const close = bucketCloseTs(exchangeTs, bucketMs);
    return close - Math.max(1, bucketMs);
}
