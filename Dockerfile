## SPDX-FileCopyrightText: 2026 Comcast Cable Communications Management, LLC
## SPDX-License-Identifier: Apache-2.0

# goreleaser builds the binary and this image from a context that already
# contains it, so there is no build stage here.  ARCH and PLATFORM are passed
# by the shared-go release workflow; buildx uses them to select the base image.
ARG PLATFORM=linux/amd64
ARG ARCH=amd64

FROM --platform=${PLATFORM} alpine:3.22

# ca-certificates is the only runtime dependency: the service speaks TLS to
# Kafka and to S3, and without the trust store both fail at connect time.
#
# No spruce.  Configuration is layered by goschtalt inside the process, so the
# image needs neither a template renderer nor an entrypoint script to render
# one.
RUN apk add --no-cache ca-certificates

# The binary, placed in the context by goreleaser.
COPY xmidt-syslog-bridge /

# Compliance details about the container and what it contains.
COPY Dockerfile /
COPY LICENSE    /
COPY NOTICE     /

# Where the service looks for configuration.  goschtalt searches, in order,
# the working directory, then $HOME/.xmidt-syslog-bridge, then here:
#
#   /etc/xmidt-syslog-bridge/xmidt-syslog-bridge.yml   a single file, or
#   /etc/xmidt-syslog-bridge/conf.d/                   fragments, merged
#
# No configuration ships in the image on purpose.  The service requires at
# least kafka.brokers, kafka.topic, kafka.group, filter.event and exactly one
# destination, so an unconfigured container exits immediately with a message
# naming what is missing rather than starting against a wrong default.
#
#   docker run -v ./my-config.yml:/etc/xmidt-syslog-bridge/xmidt-syslog-bridge.yml ...
RUN mkdir -p /etc/xmidt-syslog-bridge/conf.d

# The syslog destination writes to a Unix datagram socket on the host, which
# must be bind mounted and writable by this user:
#
#   docker run -v /dev/log:/dev/log ...
#
# The S3 destination needs no mount.
USER nobody

EXPOSE 10080
EXPOSE 9361

ENTRYPOINT ["/xmidt-syslog-bridge"]
