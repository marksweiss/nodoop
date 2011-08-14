`#!/usr/local/bin/node
// Coffee hack to include shebang literal`

Lazy = require 'lazy'
U = require './util.js'

lastWord = ''
wordTotal = 0
new Lazy(process.stdin).lines.forEach(line) -> 
  word = line.trim
  if lastWord isnt word
    U.print(lastWord + ' ' + wordTotal)  # Maps operate by side-effect, namely printing lines of output to stdout
    lastWord = word
    wordTotal = 1
  else
    wordTotal += 1
  return  # Coffee hack to generate JS function with no return value
         
process.stdin.resume()
