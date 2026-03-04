# MQ GO HTTP SDK

> Forked from [aliyunmq/mq-http-go-sdk](https://github.com/aliyunmq/mq-http-go-sdk) (upstream is no longer maintained)

Aliyun MQ Documents: http://www.aliyun.com/product/ons

Aliyun MQ Console: https://ons.console.aliyun.com

## Use

```bash
go get github.com/coldbrewtea/mq-http-go-sdk
```

```go
import mq_http_sdk "github.com/coldbrewtea/mq-http-go-sdk"
```

## Fix

Fixed `no free connections available to host` (`fasthttp.ErrNoFreeConns`) under high-concurrency workloads (e.g. batch jobs, scheduled tasks).

- `Send0()`: `fasthttp.AcquireRequest()`/`AcquireResponse()` were not paired with `ReleaseRequest()`/`ReleaseResponse()`, causing object pool leak, increased GC pressure, slower requests, and eventual connection pool exhaustion
- `initFastHttpClient()`: `MaxConnWaitTimeout` was not set, so the client returned an error immediately when no free connection was available instead of waiting

See: [aliyunmq/mq-http-go-sdk#16](https://github.com/aliyunmq/mq-http-go-sdk/issues/16)

## Note
1. Http consumer only support timer msg (less than 3 days), no matter the msg is produced from http or tcp protocol.
2. Order is only supported at special server cluster.

## Sample (github)

[Publish Message](https://github.com/aliyunmq/mq-http-samples/blob/master/go/producer.go)

[Consume Message](https://github.com/aliyunmq/mq-http-samples/blob/master/go/consumer.go)

[Transaction Message](https://github.com/aliyunmq/mq-http-samples/blob/master/go/trans_producer.go)

[Publish Order Message](https://github.com/aliyunmq/mq-http-samples/blob/master/go/order_producer.go)

[Consume Order Message](https://github.com/aliyunmq/mq-http-samples/blob/master/go/order_consumer.go)


## Sample (code.aliyun.com)

[Publish Message](https://code.aliyun.com/aliware_rocketmq/mq-http-samples/blob/master/go/producer.go)

[Consume Message](https://code.aliyun.com/aliware_rocketmq/mq-http-samples/blob/master/go/consumer.go)

[Transaction Message](https://code.aliyun.com/aliware_rocketmq/mq-http-samples/blob/master/go/trans_producer.go)

[Publish Order Message](https://code.aliyun.com/aliware_rocketmq/mq-http-samples/blob/master/go/order_producer.go)

[Consume Order Message](https://code.aliyun.com/aliware_rocketmq/mq-http-samples/blob/master/go/order_consumer.go)