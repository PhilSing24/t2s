/**
 * @file socket_utils.hpp
 * @brief Small platform-aware helpers for socket-level configuration.
 *
 * Centralizes TCP keepalive setup so both feed handlers configure their
 * sockets identically. Without keepalive, dead-but-not-RSTed connections
 * (e.g. after the host system suspends, or when an upstream load balancer
 * silently drops state) can linger for hours before ws.read() returns,
 * during which the FH appears connected but receives no data.
 *
 * On Linux we set TCP_KEEPIDLE / TCP_KEEPINTVL / TCP_KEEPCNT explicitly
 * because the kernel defaults are conservative (2 hours before first probe).
 * On other platforms we fall back to enabling keepalive at the boost level
 * with system defaults.
 */

#ifndef SOCKET_UTILS_HPP
#define SOCKET_UTILS_HPP

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/socket_base.hpp>

#include <spdlog/spdlog.h>

#ifdef __linux__
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#endif

namespace t2s {

/**
 * @brief Enable aggressive TCP keepalive on a connected socket.
 *
 * @param socket  A connected TCP socket
 * @param idleSec Seconds of idle before first probe (default 60)
 * @param intervalSec Seconds between probes (default 10)
 * @param probeCount Probes before declaring the connection dead (default 3)
 *
 * With defaults, dead connections are detected within roughly
 * idleSec + (intervalSec * probeCount) = 90 seconds.
 */
inline void applyKeepalive(boost::asio::ip::tcp::socket& socket,
                           int idleSec = 60,
                           int intervalSec = 10,
                           int probeCount = 3) {
    boost::system::error_code ec;
    socket.set_option(boost::asio::socket_base::keep_alive(true), ec);
    if (ec) {
        spdlog::warn("Failed to enable SO_KEEPALIVE: {}", ec.message());
        return;
    }

#ifdef __linux__
    int fd = socket.native_handle();

    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idleSec, sizeof(idleSec)) != 0) {
        spdlog::warn("Failed to set TCP_KEEPIDLE: errno {}", errno);
    }
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &intervalSec, sizeof(intervalSec)) != 0) {
        spdlog::warn("Failed to set TCP_KEEPINTVL: errno {}", errno);
    }
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &probeCount, sizeof(probeCount)) != 0) {
        spdlog::warn("Failed to set TCP_KEEPCNT: errno {}", errno);
    }

    spdlog::debug("Keepalive: enabled (idle={}s interval={}s probes={})",
                  idleSec, intervalSec, probeCount);
#else
    (void)idleSec; (void)intervalSec; (void)probeCount;
    spdlog::debug("Keepalive: enabled with platform defaults");
#endif
}

} // namespace t2s

#endif // SOCKET_UTILS_HPP
