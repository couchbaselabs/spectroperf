# Notes

## Various notes for running

One way to run this is with cbdinocluster+k3s.  
That's handy, since there's a CAO deployer that can also deploy with Cloud Native Gateway (CNG).

Then, to use it, you'll need to get the connstr something like this (for your cluster):

```
./cbdinocluster connstr --couchbase2 4ad65c17d04d
```

But, note that you'll need to disable TLS verification if using IP addresses and not hostnames with Subject Alternate Names.
In the gocb options:

```
		SecurityConfig: gocb.SecurityConfig{TLSSkipVerify: true},
```

## Checking CNG Health Probe

You can use the go grpc-health-probe to check to see if CNG is accessible:

```
go install github.com/grpc-ecosystem/grpc-health-probe@latest
~/go/bin/grpc-health-probe -addr=192.168.107.3:30707 -tls -tls-no-verify

```
