
Gem::Specification.new do |spec|
  spec.name          = "embulk-filter-faker"
  spec.version       = "0.1.0"
  spec.authors       = ["Shinnosuke Shinozaki"]
  spec.summary       = %[Faker filter plugin for Embulk]
  spec.description   = %[Faker]
  spec.email         = ["shinnosuke.shinozaki@gmail.com"]
  spec.licenses      = ["MIT"]
  # TODO set this: spec.homepage      = "https://github.com/shinnosuke.shinozaki/embulk-filter-faker"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r"^(test|spec)/")
  spec.require_paths = ["lib"]

  #spec.add_dependency 'YOUR_GEM_DEPENDENCY', ['~> YOUR_GEM_DEPENDENCY_VERSION']
  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
