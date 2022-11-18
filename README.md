## ballot

Consul service election with tagging support and hooks

### What is it?

Consul doesn't support leader election for registered services. This tool is meant to help with that. 
The idea is that you have multiple services and you need to select a leader. When a election change happen from active to backup 
you can hook a script execution.

### How do I test it?

```bash
$ git clone https://github.com/ncode/ballot
$ go build
$ cd examples
$ consul agent -dev -enable-script-checks=true &
$ curl --request PUT --data @my_service1.json http://127.0.0.1:8500/v1/agent/service/register\?replace-existing-checks\=true
$ curl --request PUT --data @my_service2.json http://127.0.0.1:8500/v1/agent/service/register\?replace-existing-checks\=true
```
