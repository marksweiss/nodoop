`#!/usr/local/bin/node
// Coffee hack to include shebang literal`

Lazy = require 'lazy'
U = require './util.js'

# map = (input = process.stdin) ->
  # new Lazy(input).lines.forEach(line) ->
new Lazy(process.stdin).lines.forEach(line) ->
  tkns = line.trim().split(///\s+///)
  for tkn in tkns
    U.print tkn
  return  # Coffee hack to generate JS function with no return value

process.stdin.resume()