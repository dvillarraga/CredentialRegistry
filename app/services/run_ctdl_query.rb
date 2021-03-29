require 'ctdl_query'

class RunCtdlQuery
  def self.call(
    payload,
    include_description_set_resources: false,
    include_description_sets: false,
    include_results_metadata: false,
    log: false,
    per_branch_limit: nil,
    skip: nil,
    take: nil
  )
    query_log = QueryLog.start(ctdl: payload, engine: 'ctdl') if log
    count_query = CtdlQuery.new(payload, project: 'COUNT(*) AS count')

    data_query = CtdlQuery.new(
      payload,
      project: %w["@id" "ceterms:ctid" payload],
      skip: skip,
      take: take,
      with_metadata: include_results_metadata
    )

    rows = data_query.execute

    result = {
      data: rows.map { |r| JSON(r.fetch('payload')) },
      total: count_query.execute.first.fetch('count'),
      sql: data_query.to_sql
    }

    if include_description_sets
      description_set_data = FetchDescriptionSetData.call(
        rows.map { |r| r.fetch('ceterms:ctid') }.compact,
        include_resources: include_description_set_resources,
        per_branch_limit: per_branch_limit
      )

      entity = API::Entities::DescriptionSetData.represent(description_set_data)
      result.merge!(entity.as_json)
    end

    if include_results_metadata
      result.merge!(
        results_metadata: rows.map do |r|
          {
            'resource_uri' => r.fetch('@id'),
            **r.slice('created_at', 'updated_at', 'owned_by', 'published_by')
          }
        end
      )
    end

    query_log&.update(query: data_query.to_sql)
    query_log&.complete(result)
    OpenStruct.new(result: result, status: 200)
  rescue => e
    query_log&.fail(e.message)
    Airbrake.notify(e, query: payload)

    OpenStruct.new(
      result: { error: e.message },
      status: 500
    )
  end
end
