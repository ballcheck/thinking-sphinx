class ThinkingSphinx::RealTime::Populator
  def self.populate(*args)
    new(*args).populate
  end

  def initialize(index, limit = nil)
    @index = index
    @limit = limit || Float::INFINITY
  end

  def populate(&block)
    instrument 'start_populating'

    remove_files

    cnt = 0
    scope.find_in_batches(:batch_size => batch_size) do |instances|
      raise if cnt >= limit
      cnt += instances.size
      instances = instances[0..((limit-cnt)-1)] if cnt > limit
      transcriber.copy *instances
      instrument 'populated', :instances => instances
    end

    controller.rotate
    instrument 'finish_populating'
  end

  private

  attr_reader :index, :limit

  delegate :controller, :batch_size, :to => :configuration
  delegate :scope,                   :to => :index

  def configuration
    ThinkingSphinx::Configuration.instance
  end

  def instrument(message, options = {})
    ActiveSupport::Notifications.instrument(
      "#{message}.thinking_sphinx.real_time", options.merge(:index => index)
    )
  end

  def remove_files
    Dir["#{index.path}*"].each { |file| FileUtils.rm file }
  end

  def transcriber
    @transcriber ||= ThinkingSphinx::RealTime::Transcriber.new index
  end
end
