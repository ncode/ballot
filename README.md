# Ballot

[![Go](https://github.com/ncode/ballot/actions/workflows/go.yml/badge.svg)](https://github.com/ncode/ballot/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/ncode/ballot)](https://goreportcard.com/report/github.com/ncode/ballot)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![codecov](https://codecov.io/gh/ncode/ballot/graph/badge.svg?token=GVADXAIACR)](https://codecov.io/gh/ncode/ballot)

Consul based leader election with tagging support and hooks

### What is it?

Consul lacks a built-in feature for leader election among registered services. This tool is designed to fill that gap. It functions by designating a leader among multiple services, marking the chosen leader with a specified tag. Additionally, it allows for the execution of a script whenever a leader election occurs.

### How does it work?

Ballot uses Consul's session API to create a session for each service. The session is then used to create a lock on a key. The service that successfully creates the lock is elected as the leader. The leader is then tagged with a specified tag. The leader election is monitored and the leader is updated if the current leader is no longer healthy.
More info about the sessions here [https://developer.hashicorp.com/consul/tutorials/developer-configuration/application-leader-elections](https://developer.hashicorp.com/consul/tutorials/developer-configuration/application-leader-elections).

### How do I use it?

1. Install Ballot
```bash
$ git clone https://github.com/ncode/ballot
$ go build
```
2. Run consul in dev mode and register two services
```bash
$ consul agent -dev -enable-script-checks=true &
$ curl --request PUT --data @examples/consul/my-service1.json http://127.0.0.1:8500/v1/agent/service/register\?replace-existing-checks\=true
$ curl --request PUT --data @examples/consul/my-service2.json http://127.0.0.1:8500/v1/agent/service/register\?replace-existing-checks\=true
```
3. Run one instance of Ballot for each service
```bash
$ ./ballot run --config $PWD/examples/config/ballot1.yaml &
$ ./ballot run --config $PWD/examples/config/ballot2.yaml &
```
4. Open consul ui http://127.0.0.1:8500/ui/dc1/services/my_service/instances
5. Play with the health checks and see the election happening and moving
```bash
$ cp /bin/ls /tmp/lalala1
$ cp /bin/ls /tmp/lalala2
$ sleep 30
$ rm /tmp/lalala1
$ sleep 10
$ cp /bin/ls /tmp/lalala1
$ rm /tmp/lalala2
$ sleep 10
$ cp /bin/ls /tmp/lalala2
```

### Environment variables

During the call of execOnPromote and execOnDemote a few environment variables are injected incase you need to use the and port of the service for an intended automation.

```bash
$ADDRESS   # IP Address of the current service elected
$PORT      # Port of the service
$SESSIONID # Current SessionID of the elected master
```

### Configuration

The configuration file is a yaml file with the following structure:

```yaml
consul:
  token:                                    # Consul token
election:
  enabled:
    - my-service-name                       # Name of the service enabled for election
  services:
    my-service-name:                        # Name of the service
      id: my-service-name                   # ID of the service
      key: my-service-name                  # Key to be used for the lock in Consul, this should be the same across all nodes
      token:                                # Token to be used for the session in Consul
      serviceChecks:                        # List of checks to be used to determine the health of the service
        - ping                              # Name of the check
      primaryTag: primary                   # Tag to be used to mark the leader
      execOnPromote: '/bin/echo primary'    # Command to be executed when the service is elected as leader
      execOnDemote: '/bin/echo secondary'   # Command to be executed when the service is demoted as leader
      ttl: 10s                              # TTL for the session
      lockDelay: 5s                         # Lock delay for the session
```

### TODO:

- Write more tests
- Add more examples
- Re-enable the hooks on state change
- Allow to pre-define the preferred leader
- Update the docks with the lock delays and timeouts
