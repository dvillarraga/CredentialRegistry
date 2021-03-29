require 'run_ctdl_query'

module API
  module V1
    # CTDL endpoint
    class Ctdl < Grape::API
      helpers SharedHelpers

      before do
        authenticate!
      end

      desc 'Executes a CTDL query'
      params do
        optional :include_description_set_resources, default: false, type: Boolean
        optional :include_description_sets, default: false, type: Boolean
        optional :include_results_metadata, default: false, type: Boolean
        optional :log, default: true, type: Boolean
        optional :per_branch_limit, type: Integer
        optional :skip, default: 0, type: Integer
        optional :take, default: 10, type: Integer
      end
      post '/ctdl' do
        query = JSON(request.body.read)
        request.body.rewind

        response = RunCtdlQuery.call(
          query,
          include_description_set_resources: params[:include_description_set_resources],
          include_description_sets: params[:include_description_sets],
          include_results_metadata: params[:include_results_metadata],
          log: params[:log],
          per_branch_limit: params[:per_branch_limit],
          skip: params[:skip],
          take: params[:take]
        )

        status response.status
        response.result
      end
    end
  end
end
