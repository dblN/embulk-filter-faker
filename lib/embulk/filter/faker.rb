Embulk::JavaPlugin.register_filter(
  "faker", "org.embulk.filter.faker.FakerFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
