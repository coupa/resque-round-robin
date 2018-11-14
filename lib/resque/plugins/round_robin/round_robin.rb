
module Resque::Plugins
  module RoundRobin
    class Configuration
      attr_accessor :max_qeueue_workers
       def initialize
        @max_qeueue_workers = {}
      end
    end

    def self.configuration
      @configuration ||= Configuration.new
    end

    def self.configure
      yield(configuration)
    end

    def rotated_queues
      @n ||= 0
      @n += 1
      rot_queues = queues # since we rely on the resque-dynamic-queues plugin, this is all the queues, expanded out
      if rot_queues.size > 0
        @n = @n % rot_queues.size
        rot_queues.rotate(@n)
      else
        rot_queues
      end
    end

    def queue_depth(queuename)
      busy_queues = Resque::Worker.working.map { |worker| worker.job["queue"] }.compact
      # find the queuename, count it.
      busy_queues.select {|q| q == queuename }.size
    end
    
    def queue_empty?(queuename)
      Resque.data_store.queue_size(queuename) == 0
    end

    DEFAULT_QUEUE_DEPTH = 20
    def max_qeueue_workers_for(queuename)
      default_max_workers = DEFAULT_QUEUE_DEPTH
      unless ENV["RESQUE_QUEUE_DEPTH"].nil? || ENV["RESQUE_QUEUE_DEPTH"] == ""
        default_max_workers = ENV["RESQUE_QUEUE_DEPTH"].to_i
      end

      max_qeueue_workers = Resque::Plugins::RoundRobin.configuration.max_qeueue_workers
      max_qeueue_workers.fetch(queuename.scan(/_([^_]*)/)&.last&.first, default_max_workers)
    end

    def should_work_on_queue?(queuename)
      return true if @queues.include?('*')  # workers with QUEUES=* are special and are not subject to queue depth setting
      if queue_empty?(queuename)
        # Immediately remove empty queue, to remove clutter in resque-web and
        # to avoid inefficiency looking at empty queues.
        Resque.data_store.remove_queue(queuename)
        return false
      end
      
      cur_depth = queue_depth(queuename)
      max = max_qeueue_workers_for(queuename)
      return true if max == 0 # 0 means no limiting

      log! "queue #{queuename} depth = #{cur_depth} max = #{max}"
      return true if cur_depth < max
      false
    end

    def reserve_with_round_robin
      qs = rotated_queues
      qs.each do |queue|
        log! "Checking #{queue}"
        if should_work_on_queue?(queue) && job = Resque::Job.reserve(queue)
          log! "Found job on #{queue}"
          return job
        end
        # Start the next search at the queue after the one from which we pick a job.
        @n += 1
      end

      nil
    rescue Exception => e
      log "Error reserving job: #{e.inspect}"
      log e.backtrace.join("\n")
      raise e
    end

    def self.included(receiver)
      receiver.class_eval do
        alias reserve_without_round_robin reserve
        alias reserve reserve_with_round_robin
      end
    end

  end # RoundRobin
end # Resque::Plugins

