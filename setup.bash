#!/bin/bash
mmm -c $1 replicate --src=server_a/test.foo --dst=server_b/test.foo
mmm -c $1 replicate --src=server_a/test.bar --dst=server_b/test.bar
mmm -c $1 replicate --src=server_b/test.foo --dst=server_a/test.foo
mmm -c $1 replicate --src=server_b/test.bar --dst=server_a/test.bar
