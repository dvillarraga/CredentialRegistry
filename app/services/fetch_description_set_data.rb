# Fetches description set data for the given CTIDs
class FetchDescriptionSetData
  def self.call(
    ctids,
    include_resources: false,
    include_results_metadata: false,
    path_contains: nil,
    path_exact: nil,
    per_branch_limit: nil
  )
    description_sets = DescriptionSet
      .where(ceterms_ctid: ctids)
      .select(:ceterms_ctid, :path)
      .select('cardinality(uris) total')
      .order(:ceterms_ctid, :path)

    if path_exact.present?
      description_sets.where!('LOWER(path) = ?', path_exact.downcase)
    elsif path_contains.present?
      description_sets.where!("path ILIKE '%#{path_contains}%'")
    end

    description_sets =
      if per_branch_limit
        description_sets.select("uris[1:#{per_branch_limit}] uris")
      else
        description_sets.select(:uris)
      end

   description_set_groups = description_sets
    .group_by(&:ceterms_ctid)
    .map do |group|
      OpenStruct.new(ctid: group.first, description_set: group.last)
    end

    if include_resources
      ids = description_sets.map(&:uris).flatten.uniq.map do |uri|
        id = uri.split('/').last
        next id unless uri.starts_with?('https://credreg.net/bnodes/')

        "_:#{id}"
      end

      resource_relation = EnvelopeResource
        .not_deleted
        .where(resource_id: ids)
        .select(:processed_resource, :resource_id)

      if include_results_metadata
        resource_relation = resource_relation
          .joins(:envelope)
          .left_joins(envelope: %i[organization publishing_organization])
          .select(
            'envelopes.created_at, ' \
            'envelopes.updated_at, ' \
            'organizations._ctid owned_by, ' \
            'publishing_organizations_envelopes._ctid published_by'
          )
      end

      resources = []
      results_metadata = [] if include_results_metadata

      resource_relation.map do |resource|
        resources << resource.processed_resource
        next unless include_results_metadata

        results_metadata << {
          resource_uri: resource.resource_id,
          created_at: resource.created_at,
          updated_at: resource.updated_at,
          owned_by: resource.owned_by,
          published_by: resource.published_by
        }
      end
    end

    OpenStruct.new(
      description_sets: description_set_groups,
      resources: resources,
      results_metadata: results_metadata
    )
  end
end
