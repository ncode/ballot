## Ballot

Consul service election with tagging support and hooks

### What is it?

Consul doesn't support leader election for registered services. This tool is meant to help with that. 
The idea is that you have multiple services and you need to select a leader. It will apply a tag of choise to the leader and
when a election happens you can hook a script execution.
Something like a very, very simplified idea of keepalived using consul.

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

### Current state of this project?

Works on my machine.

### TODO:

- Testing using Consul Token
- Write tests
- Add a proper help for the cli
- Cleanup the code and test with a real consul setup
- Allow to pre-define the preferred leader service like on keepalived
