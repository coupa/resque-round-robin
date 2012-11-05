
module Resque::Plugins
  module RoundRobin
    def filter_busy_queues qs
      busy_queues = Resque::Worker.working.map { |worker| worker.job["queue"] }.compact
      Array(qs.dup).compact - busy_queues
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

    def queue_depth queuename
      busy_queues = Resque::Worker.working.map { |worker| worker.job["queue"] }.compact
      # find the queuename, count it.
      busy_queues.select {|q| q == queuename }.size
    end

    DEFAULT_QUEUE_DEPTH = 0
    def should_work_on_queue? queuename
      return true if @queues.include? '*'  # workers with QUEUES=* are special and are not subject to queue depth setting
      max = DEFAULT_QUEUE_DEPTH
      unless ENV["RESQUE_QUEUE_DEPTH"].nil? || ENV["RESQUE_QUEUE_DEPTH"] == ""
        max = ENV["RESQUE_QUEUE_DEPTH"].to_i
      end
      return true if max == 0 # 0 means no limiting
      cur_depth = queue_depth(queuename)
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

