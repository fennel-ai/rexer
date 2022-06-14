#!/bin/bash

pods=$(kubectl get po -A -l linkerd.io/control-plane-ns -ojsonpath="{range .items[*]}{.metadata.name} {.metadata.namespace}{'\n'}{end}")

IFS=" "

while read name namespace; do
	tcp=$(kubectl exec -n $namespace $name -c linkerd-proxy -- cat /proc/net/tcp)
	close_wait=$(echo $tcp | awk 'BEGIN {cnt=0} $4==08 {cnt++} END {print cnt}')
	fin_wait_2=$(echo $tcp | awk 'BEGIN {cnt=0} $4==05 {cnt++} END {print cnt}')

	if [ "$close_wait" -gt "0" -o "$fin_wait_2" -gt "0" ]; then
		echo "$name.$namespace has $close_wait sockets in CLOSE_WAIT and $fin_wait_2 sockets in FIN_WAIT_2"
	else
		echo "$name.$namespace is okay"
	fi

done <<< "$pods"
