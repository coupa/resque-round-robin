
require 'resque'
require 'resque/worker'
require "resque/plugins/round_robin/version"
require "resque/plugins/round_robin/round_robin"

Resque::Worker.send(:include, Resque::Plugins::RoundRobin)
