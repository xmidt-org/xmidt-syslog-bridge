<!-- SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# Write raw syslog bytes to the transport, not through a syslog client API

The syslog destination is written by opening the syslog transport (local system socket, UDP, or TCP) and writing the device's syslog message bytes unchanged. We deliberately do not use Go's `log/syslog` writer or any equivalent client API, because those prepend their own PRI, timestamp, hostname, and tag, which would wrap the device's already-complete syslog message inside a second one stamped with the cloud host's identity. Writing raw bytes lets the receiving daemon parse the device's own header, so the store attributes the message to the device, which is the point of the whole path.
