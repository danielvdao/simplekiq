# frozen_string_literal: true

require "forwardable"

module Simplekiq
  module OrchestrationJob
    include Sidekiq::Job

    extend Forwardable

    def_delegators :orchestration, :run, :in_parallel

    def perform(*args)
      perform_orchestration(*args)

      validate_sidekiq_job_included!
      # This makes it so that if there is a parent batch which this orchestration is run under, then the layered batches will be:
      # parent_batch( orchestration_batch( batch_of_first_step_of_the_orchestration ) )
      # If there is no parent batch, then it will simply be:
      # orchestration_batch( batch_of_first_step_of_the_orchestration )
      conditionally_within_parent_batch do
        OrchestrationExecutor.execute(args: args, job: self, workflow: orchestration.serialized_workflow)
      end
    end

    def workflow_plan(*args)
      perform_orchestration(*args)
      orchestration.serialized_workflow
    end

    private

    def validate_sidekiq_job_included!
      orchestration.serialized_workflow.flatten.each do |step|
        if !Object.const_get(step["klass"]).include?(Sidekiq::Job)
          raise "Sidekiq::Job must be included in the job class"
        end
      end
    end

    def conditionally_within_parent_batch
      if batch
        batch.jobs do
          yield
        end
      else
        yield
      end
    end

    def orchestration
      @orchestration ||= Orchestration.new
    end
  end
end
