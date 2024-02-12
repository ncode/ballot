# Ballot

[![Go](https://github.com/ncode/ballot/actions/workflows/go.yml/badge.svg)](https://github.com/ncode/ballot/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/ncode/ballot)](https://goreportcard.com/report/github.com/ncode/ballot)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![codecov](https://codecov.io/gh/ncode/bedel/graph/badge.svg?token=GVADXAIACR)](https://codecov.io/gh/ncode/ballot)

Consul based leader election with tagging support and hooks

### What is it?

Consul lacks a built-in feature for leader election among registered services. This tool is designed to fill that gap. It functions by designating a leader among multiple services, marking the chosen leader with a specified tag. Additionally, it allows for the execution of a script whenever a leader election occurs.

### How do I test it?

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

### TODO:

- Write tests
- Add more examples
- Re-enable the hooks on state change
- Allow to pre-define the preferred leader
- Update the docks with the lock delays and timeouts
