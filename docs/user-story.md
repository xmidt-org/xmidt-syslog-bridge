<!-- SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# User Story: AppArmor Alerts via WRP

**Wire protocol:** [Syslog over WRP](./protocol.md) (v2.0, Proposed)

## Epic

**As a** security engineer operating a fleet of RDK CPE devices,
**I want** AppArmor alerts generated on a device to arrive in a central store exactly as AppArmor emitted them,
**so that** policy violations across the fleet can be detected and investigated without shell access to individual devices.

### Why this matters

AppArmor already writes useful alerts to syslog on every device, but those alerts stay on the box where nobody sees them. The devices already maintain a secure, always-on connection to the cloud through Parodus and xmidt. Reusing that path means no new ports, credentials, or agents on the device, and no new ingress on the server side. The syslog content is never parsed or altered in transit, so tools that already understand AppArmor's output keep working unchanged.

### Scope

This story ships the AppArmor use case. Generic syslog forwarding over WRP is a recognized follow-up; the destination filter in Story 2 is the seam it will use. Both the syslog and S3 destinations are in scope.

---

## Story 1: Forward AppArmor syslog from the device

**As a** CPE platform developer,
**I want** a small on-device service (`syslog-to-xmidt`) that AppArmor can send syslog messages to and that forwards them through Parodus,
**so that** AppArmor alerts leave the device over the existing xmidt connection with no changes to AppArmor itself.

### Acceptance criteria

**Listening for messages**

- Given the service is configured for one transport, when it starts, then it listens on that transport. At least one of Unix domain datagram socket, UDP, or TCP must be supported, and the choice is made by configuration.
- Given UDP is configured, then messages are framed per RFC 5426.
- Given TCP is configured, then messages are framed per RFC 6587.
- Given a Unix domain socket is configured, then each datagram is treated as one complete syslog message, matching conventional `/dev/log` behavior.
- Given any message arrives, then it is accepted as-is. The service applies no filtering, parsing, or validation. What to send is AppArmor's decision.

**Wrapping the message**

- Given a syslog message is received, then it is wrapped in a WRP SimpleEvent (msg_type 4) with:
  - `dest` of `event:apparmor/alert/mac:<device_mac>/<unix_timestamp>`
  - `source` of `mac:<device_mac>/apparmor-forwarder`
  - `content_type` of `application/octet-stream`
  - `qos` of `90`
  - `payload` containing the raw syslog bytes
- Given the device MAC is provided by configuration, then it appears as 12 lowercase hex digits with no separators.
- Given a message is received, then the timestamp in `dest` is the Unix epoch second at which the service received it.

**Compression**

- Given a payload larger than 1024 bytes, then the payload is gzip-compressed and the WRP `headers` field contains `content-encoding: gzip`.
- Given a payload of 1024 bytes or smaller, then the payload is sent uncompressed and no `content-encoding` header is present.

**Delivery to Parodus**

- Given the service is built in C or Go, then it uses libparodus or libpdgo respectively, registering with the service name `apparmor-forwarder`.
- The service does not touch nanomsg sockets or the Parodus wire protocol directly.

**Store and forward**

- Given Parodus is unavailable (not running, disconnected, or rejecting messages), when a message arrives, then it is persisted to local storage so it survives a restart of the service.
- Given a configured maximum buffer size, when the buffer is full, then the oldest messages are dropped first.
- Given Parodus becomes available, then buffered messages are drained oldest-first.
- Given a message has been handed to Parodus successfully, then the service's responsibility for it ends. Parodus owns delivery to the cloud from there.

---

## Story 2: Deliver AppArmor syslog to its destination

**As a** cloud platform engineer operating an xmidt backend,
**I want** a service (`xmidt-syslog-bridge`) that consumes AppArmor WRP events from Kafka and writes them to either the local syslog daemon or an S3 bucket,
**so that** device alerts land in a syslog store for investigation, or in object storage for deployments that need archival.

### Acceptance criteria

**Consuming from Kafka**

- Given a configured Kafka topic, then the service consumes msgpack-encoded WRP messages from it in a consumer group, committing offsets manually. Autocommit is never enabled.
- Given a consumed record, then its value is decoded as WRP. The record key and headers are ignored; they are producer-side conventions the service must not depend on.
- Given the destination write for a message has not succeeded, then that message's offset is not committed. The service keeps no local spool (ADR 0001).

**Ignoring records that are not ours**

- Given a record whose WRP destination does not name the configured event, then it is skipped, counted by reason, logged at warn with partition and offset, and its offset is committed.
- Given a record that does not decode as WRP, carries an empty payload, or declares `content-encoding: gzip` but does not gunzip, then it is likewise skipped, counted, logged, and committed.
- Given any such record, then it never blocks delivery of the valid records behind it on its partition.

**Choosing a destination**

- Given the service configuration, then exactly one destination is active: syslog or S3. Configuring neither or both is a startup error.
- Given both destinations are wanted for one topic, then two instances run under different consumer groups.

**Decompression**

- Given a message whose headers include `content-encoding: gzip`, then the payload is gunzipped before output.
- Given a message with no such header, then the payload is output as received.
- In both cases the output is byte-for-byte the syslog message AppArmor originally emitted.

**Syslog destination**

- Given a configured Unix domain datagram socket path, defaulting to `/dev/log`, then each message's raw bytes are written to it, one datagram per message.
- The service does not use a syslog client API that adds its own header. The device's own PRI, timestamp, and hostname are what the receiving daemon sees (ADR 0002).
- Network syslog transports are deliberately absent. Only the Unix socket provides the backpressure the delivery guarantee depends on (ADR 0003).

**S3 destination**

- Given a configured bucket, then messages accumulate per Kafka partition and are written as one object per batch.
- Given a batch, then it closes on the first of a configured message count, a configured timeout measured from its first message, a configured byte size, losing ownership of its partition, or graceful shutdown.
- Given a message count of 1, then each message is its own object. There is no separate single-object mode.
- Given an object, then it holds each message's raw bytes followed by a newline, with nothing escaped. A message containing a newline therefore reads as two lines; consumers must account for this (ADR 0005).
- Given a configured object key template, then each object's key is rendered from the batch's flush time, that time's millisecond component, message count, instance identifier, and file extension. The service never reuses a flush timestamp, so keys cannot collide.
- Given a partition and offset range, then they are recorded as object metadata rather than in the key.
- Given the process dies with an open batch, then those messages are replayed from Kafka, never lost. Objects written after a replay may overlap in content.

**Surviving a destination outage**

- Given a write fails for a transient reason, then the service pauses fetching from Kafka, retries with backoff until it succeeds, and commits nothing in the meantime (ADR 0006).
- Given a write fails for a reason that can never succeed, such as a message larger than the socket will ever accept, then the message is dropped, counted, logged at error, and its offset committed, so that one message cannot block its partition forever (ADR 0004).
- Given a destination is unreachable, then the service remains live. Liveness must not depend on destination reachability, or every instance in the group restarts at once during an outage no restart can fix.

---

## Definition of done

- Both components are configurable as described and documented.
- A message sent by AppArmor on a device appears in the syslog store with identical bytes, both for payloads under and over 1 KB, **up to the receiving daemon's configured message size**. rsyslog's `global(maxMessageSize)` defaults to 8 KB and silently truncates above it; raising it to cover the largest expected alert is a deployment prerequisite the bridge cannot detect or enforce.
- The same message written to S3 is byte-identical when read back, both when batched and at a message count of 1.
- Killing the service with an open S3 batch results in replay, not loss, and this is covered by a test.
- A record that cannot be decoded does not block delivery of valid records behind it on its partition, and this is covered by a test.
- Configuring zero or two destinations fails at startup, and this is covered by a test.
- A message sent while Parodus is down on the device is delivered after Parodus reconnects, and survives a restart of `syslog-to-xmidt` in between.
- Buffer limits on the device are enforced and oldest-first eviction is observable in tests.

## Out of scope

- Generic (non-AppArmor) syslog routing over WRP. The destination filter is the seam it would use.
- Caduceus webhooks as an event source. Considered and dropped: a webhook response cannot wait on an S3 batch.
- Network syslog transports — UDP, TCP, and TLS (ADR 0003).
- Any parsing, filtering, escaping, or truncation of syslog content at any hop (ADR 0005).
- Any local spool in `xmidt-syslog-bridge`. Kafka is the buffer (ADR 0001).
- Deduplication. Delivery is at-least-once, so duplicates are permitted by definition.

## Open questions

- Should the store-and-forward buffer size default on the device be defined in the spec, or left entirely to deployment configuration?
