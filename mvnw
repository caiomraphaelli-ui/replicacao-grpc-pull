#!/bin/sh
# Maven Wrapper simplificado (Linux/macOS): baixa o Apache Maven na primeira vez e executa.
set -e
MVN_VERSION=3.9.11
MVN_BASE="$HOME/.m2/wrapper/dists/apache-maven-$MVN_VERSION"
MVN_CMD="$MVN_BASE/apache-maven-$MVN_VERSION/bin/mvn"
MVN_URL="https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/$MVN_VERSION/apache-maven-$MVN_VERSION-bin.tar.gz"
if [ ! -x "$MVN_CMD" ]; then
  echo "Baixando Apache Maven $MVN_VERSION - apenas na primeira execucao..."
  mkdir -p "$MVN_BASE"
  if command -v curl >/dev/null 2>&1; then curl -fsSL "$MVN_URL" -o "$MVN_BASE/maven.tgz"; else wget -q "$MVN_URL" -O "$MVN_BASE/maven.tgz"; fi
  tar xzf "$MVN_BASE/maven.tgz" -C "$MVN_BASE" && rm "$MVN_BASE/maven.tgz"
fi
exec "$MVN_CMD" "$@"
