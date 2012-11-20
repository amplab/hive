#!/usr/bin/env python

# Add
import sys
import os

args = sys.argv[1:]

if len(args) != 5:
  print >> sys.stderr, "Arguments: fileName groupId artifactId version scope"
  print >> sys.stderr, "Provided arguments: " + " ".join(args)
  sys.exit(1)

fileName, groupId, artifactId, version, scope = args
f = open(fileName)
lines = [l for l in f]
f.close()

f = open(fileName, "wt")
for l in lines:
  l = l.rstrip()
  l_trim = l.strip()
  if l_trim == "</dependencies>":
    print >> f, """
    <dependency>
      <groupId>%s</groupId>
      <artifactId>%s</artifactId>
      <version>%s</version>
      <scope>%s</scope>
    </dependency>
""" % (groupId, artifactId, version, scope)
  print >> f, l

f.close()

print "Added dependency to %s: %s:%s:%s:%s" % (fileName, groupId, artifactId,
    version, scope)
