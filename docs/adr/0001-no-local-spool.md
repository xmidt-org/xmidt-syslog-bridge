<!-- SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# xmidt-syslog-bridge keeps no local spool

xmidt-syslog-bridge is stateless across restarts. It acknowledges an event to its source (Kafka offset commit, HTTP 2xx to Caduceus) only after the write to the destination succeeds, and never persists events to disk. The only in-memory holding is an open S3 batch, which is by definition unacknowledged: a crash replays it, never loses it. During a destination outage the source accumulates the backlog and consumer lag or Caduceus retry counts are the operational signal. We chose this over a local spool because both sources already provide durable buffering, a spool would make the service stateful and complicate horizontal scaling, and a spool that fills still has to drop or block, so it only moves the problem.
