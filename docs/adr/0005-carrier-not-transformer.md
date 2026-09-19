<!-- SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->
# We are the carrier, not a transformer: no escaping in S3 objects

An **S3 Destination** object holds its **Batch** as raw **Syslog Message** bytes with a newline after each one. We do not escape anything. Backslash escaping (`\` to `\\`, newline to `\n`) and JSON Lines were both considered and rejected: each would make xmidt-syslog-bridge the one hop on the whole path that alters the bytes it carries, and the path exists precisely so that the message a device emitted is the message that arrives.

The consequence is deliberate and must not be "fixed" later. A **Syslog Message** that itself contains a newline appears in the object as two lines, and a reader splitting on newlines will see it as two records. We accept that in exchange for the object being a plain log file that `zgrep` works on and that no consumer needs a decoder for. A future reader who wants message boundaries preserved should add length-prefix framing as a separate object format rather than introduce escaping, because escaping trades one fidelity loss for another while also breaking every existing consumer.
