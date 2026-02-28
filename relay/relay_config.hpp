/**
 * NA ZMQ Relay — TOML configuration loader
 *
 * Config format — a single file shared by all relays:
 *
 *   [peers.alpha]
 *   address = "tcp://login-alpha:5555"
 *   identity = "relay-alpha"
 *
 *   [peers.beta]
 *   address = "tcp://login-beta:5555"
 *   identity = "relay-beta"
 *
 * Each relay is started with:  na_zmq_relay <config.toml> <cluster>
 * It binds using its own entry and connects to every other peer.
 */

#ifndef NA_ZMQ_RELAY_CONFIG_HPP
#define NA_ZMQ_RELAY_CONFIG_HPP

#include <map>
#include <stdexcept>
#include <string>

#include <toml.hpp>

struct PeerInfo {
    std::string address;  /* "tcp://host:port" */
    std::string identity; /* ZMQ routing identity */
};

struct RelayConfig {
    std::string cluster;      /* This relay's cluster name */
    std::string bind_address; /* Address to bind (from own peer entry) */
    std::string identity;     /* ZMQ routing identity (from own peer entry) */
    std::map<std::string, PeerInfo> peers; /* Remote peers to connect to */
};

inline RelayConfig
load_config(const std::string &path, const std::string &cluster)
{
    RelayConfig cfg;
    auto data = toml::parse(path);

    const auto &peers = toml::find(data, "peers");
    const auto &table = peers.as_table();

    auto self_it = table.find(cluster);
    if (self_it == table.end())
        throw std::runtime_error(
            "cluster '" + cluster + "' not found in [peers]");

    cfg.cluster = cluster;
    cfg.bind_address = toml::find<std::string>(self_it->second, "address");
    cfg.identity = toml::find<std::string>(self_it->second, "identity");

    for (const auto &[name, val] : table) {
        if (name == cluster)
            continue; /* Skip self */
        PeerInfo pi;
        pi.address = toml::find<std::string>(val, "address");
        pi.identity = toml::find<std::string>(val, "identity");
        cfg.peers[name] = std::move(pi);
    }

    return cfg;
}

#endif /* NA_ZMQ_RELAY_CONFIG_HPP */
