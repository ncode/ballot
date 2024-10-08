service {
  id      = "my-service1"
  name    = "my-service"
  tags    = ["v1"]
  address = "127.0.0.1"
  port    = 8000

  enable_tag_override = true

  check {
    ID                             = "service:election1"
    DeregisterCriticalServiceAfter = "90m"
    Args                           = ["/bin/ls", "/state/ready1"]
    Interval                       = "10s"
    timeout                        = "5s"
  }

  weights {
    passing = 10
    warning = 1
  }
}
