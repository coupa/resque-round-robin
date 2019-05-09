# -*- encoding: utf-8 -*-
require File.expand_path('../lib/resque/plugins/round_robin/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Eddy Kim"]
  gem.email         = ["eddyhkim@gmail.com"]
  gem.description   = %q{A Resque round-robin plugin}
  gem.summary       = %q{A Resque plugin to modify the worker behavior to pull jobs off queues, round-robin}
  gem.homepage      = ""

  gem.add_dependency "resque"

  gem.add_development_dependency('rspec', '~> 2.5')
  gem.add_development_dependency('rack-test', '~> 0.5.4')

  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.files         = `git ls-files`.split("\n")
  gem.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.name          = "resque-round-robin"
  gem.require_paths = ["lib"]
  gem.version       = Resque::Plugins::RoundRobin::VERSION
end
