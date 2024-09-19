#!/usr/bin/env bash

if [ ! -f "$1" ]; then
  echo "args 1 is not found file"
fi

version=$(cat "$1")

low=${version##*.}
m=${version%.*}
high=${m%.*}
mid=${m#*.}

low=$((low + 1))
if [ $low -ge 1000 ]; then
  mid=$((mid + 1))
  low=1
fi

if [ "$mid" -ge 100 ]; then
    high=$((high + 1))
    mid=1
fi

buildversion="$high.$mid.$low"

echo "$buildversion" > "$1"

echo "$buildversion"