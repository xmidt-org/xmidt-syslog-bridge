<!-- SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# xmidt-syslog-bridge

The cloud-side half of the AppArmor-over-WRP path. It receives WRP events that carry syslog messages captured on CPE devices and delivers them, unaltered, to a **Destination**. This repository owns the server component and the design documentation for the whole path.

## Language

**xmidt-syslog-bridge**:
The server component. Receives WRP events from the xmidt backend and delivers the carried **Syslog Message** to its one **Destination**, whether that is syslog or S3. It is a carrier and a delivery agent, never a transformer.
_Avoid_: xmidt-to-syslog (the former name, still in v1.0 of the spec; it also wrongly implies syslog is the only output), the consumer, the forwarder, the service (ambiguous with the device component)

**syslog-to-xmidt**:
The device component. Runs on the CPE, accepts syslog messages locally, wraps each in a WRP event, and hands it to Parodus. Lives in a separate repository; a Go implementation may later be added here.
_Avoid_: the agent, the client, apparmor-forwarder (that is its Parodus service name, not the component)

**Event Source**:
Where **xmidt-syslog-bridge** obtains WRP events. In this release the only source is a **Kafka** topic. Caduceus webhooks were considered and dropped because holding a webhook response for an S3 **Batch** is not viable.
_Avoid_: listener, receiver, input

**Kafka** (event source):
A Kafka topic carrying WRP events as msgpack-encoded record values, as published by Talaria through the wrpkafka library. The topic is **dedicated** to AppArmor events: Talaria's topic routing sends only events whose WRP destination names the `apparmor` event to it. xmidt-syslog-bridge consumes from it in a consumer group. Several instances may consume the same topic under different groups to feed different **Destinations**.

**Foreign Record**:
A record on the **Kafka** topic that xmidt-syslog-bridge will not deliver: its WRP destination does not name the expected event, it does not decode as WRP, or its payload is empty or fails to gunzip. Because the topic is dedicated, a foreign record means something upstream is misconfigured, not that routine filtering is doing its job.
_Avoid_: bad message, poison record, garbage

**Syslog Message**:
The opaque bytes captured on the device, exactly as the device's syslog producer emitted them. Never parsed, validated, escaped, truncated, or altered anywhere on the path: xmidt-syslog-bridge is a carrier and a delivery agent, nothing more. Distinct from the datagram or object that carries it.
_Avoid_: log line, event (that is the WRP wrapper), record

**Destination**:
Where xmidt-syslog-bridge writes each **Syslog Message**. Exactly one is active per running instance: either a **Syslog Destination** or an **S3 Destination**. To feed both, run two instances against the same Kafka topic with different consumer groups.
_Avoid_: sink, output, output target, storage

**Syslog Destination**:
The rsyslog instance on the same host, reached over a Unix domain datagram socket at a configured path (`/dev/log` by default). Each **Syslog Message** is written as raw bytes, one datagram per message. Remote syslog transports are deliberately absent; see ADR 0003.
_Avoid_: syslog server, syslog store, syslog transport

**S3 Destination**:
An S3 bucket that receives **Syslog Messages** as objects, one object per **Batch**. Inside an object, each message is followed by a newline and is otherwise untouched; nothing is escaped, so a message that itself contains a newline reads as two lines (see ADR 0005). Objects are optionally gzip-compressed. The object's path within the bucket is produced by an operator-supplied **Object Key Template**. Objects are written with a single PUT, so a reader never sees a partially written one; after a crash, objects may overlap in content.

**Object Key Template**:
A Go text template the operator configures to decide the folder structure and file name of each S3 object within the fixed, configured bucket. It is evaluated against facts about the **Batch** being written: flush time, its millisecond component, message count, **Instance Identifier**, and file extension. Kafka partition and offsets are deliberately absent from the key; they travel as S3 object metadata instead, where they answer reconciliation questions without dictating the folder structure.
_Avoid_: path pattern, naming scheme, prefix (the prefix is just whatever the template starts with)

**Instance Identifier**:
An operator-supplied name for one running instance of xmidt-syslog-bridge, defaulting to the hostname. It is the only thing separating the object keys of two instances writing to the same bucket, so it must be unique across them. Together with a flush time the instance guarantees never to reuse, it makes every object key unique.
_Avoid_: node name, host, pod (those are where it usually comes from, not what it is)

**Batch**:
A group of one or more **Syslog Messages** held in memory for one Kafka partition and written to the **S3 Destination** as a single object. A batch closes on the first of: a configured message count, a configured age measured from its first message, a configured byte size, losing ownership of its partition, or graceful shutdown. There is no separate one-object-per-message mode; a message count of 1 is how you get one. Nothing in a batch is acknowledged to the **Event Source** until its object is written, so a crash with an open batch replays rather than loses.
_Avoid_: buffer, spool, chunk

**Delivery Guarantee**:
At-least-once, bounded by the **Event Source**. xmidt-syslog-bridge commits a Kafka offset only after the write to the **Destination** succeeds, or for a batched **S3 Destination**, after the whole **Batch** is written. It keeps no local spool; the source is the buffer. Duplicates are permitted by definition. The single exception is a **Permanently Undeliverable Message**.

**Permanently Undeliverable Message**:
A **Syslog Message** whose write can never succeed however long it is retried, because it exceeds what the **Syslog Destination** socket will accept. It is dropped, counted, and logged at error, and its offset is committed, so that one message cannot block its partition forever. Distinct from a transient write failure, which is retried indefinitely. See ADR 0004.
_Avoid_: poison pill, bad message, oversized message (size is the cause, not the category)
