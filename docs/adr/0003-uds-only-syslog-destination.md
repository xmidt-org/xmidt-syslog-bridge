<!-- SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# The syslog destination is the local Unix domain socket only

xmidt-syslog-bridge writes to rsyslog on the same host over a Unix domain datagram socket, and supports no network syslog transport. UDP and TCP were both considered and deferred: forwarding to an rsyslog on another machine does not serve the deployment, and of the three transports only the Unix datagram socket can honor the at-least-once guarantee of ADR 0001. When rsyslog's queue fills, a write to the Unix socket blocks or returns `EAGAIN`, so the offset is not committed and consumer lag becomes the signal; a UDP datagram to a full receive buffer is dropped by the kernel while `write` reports success, which would commit an offset for a message that never arrived. TCP gives backpressure but reintroduces framing (RFC 6587 octet-counting versus newline-delimited), connection lifecycle, and partial writes, at roughly eight times the implementation cost of the datagram path.

The destination is configured in the standard library's `network` plus `address` shape, so adding `udp` or `tcp` later is an enum extension rather than a redesign. A future UDP transport must be documented as best-effort, not as a peer of the Unix socket.
