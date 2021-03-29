require 'indexed_envelope_resource'
require 'indexed_envelope_resource_reference'
require 'json_context'
require 'postgres_ext'

# Executes a CTDL query over indexed envelope resources
class CtdlQuery
  ANY_VALUE = 'search:anyValue'.freeze
  IMPOSSIBLE_CONDITION = Arel::Nodes::InfixOperation.new('=', 0, 1)

  SearchValue = Struct.new(:items, :operator, :match_type)

  TYPES = {
    'xsd:boolean' => :boolean,
    'xsd:date' => :date,
    'xsd:decimal' => :decimal,
    'xsd:dateTime' => :datetime,
    'xsd:float' => :float,
    'xsd:integer' => :integer
  }.freeze

  attr_reader :condition, :name, :projections, :query, :ref, :reverse_ref, :skip,
              :subqueries, :subresource_uri_values, :table, :take, :with_metadata

  delegate :columns_hash, to: IndexedEnvelopeResource
  delegate :context, to: JsonContext

  def initialize(
    query,
    name: nil,
    project: [],
    ref: nil,
    reverse_ref: false,
    skip: nil,
    take: nil,
    with_metadata: false
  )
    @name = name
    @projections = Array(project)
    @query = query
    @ref = ref
    @reverse_ref = reverse_ref
    @skip = skip
    @subqueries = []
    @table = IndexedEnvelopeResource.arel_table
    @take = take
    @with_metadata = with_metadata

    @condition = build(query) unless subresource_uri_values
  end

  def execute
    IndexedEnvelopeResource.connection.execute(to_sql)
  end

  def to_sql
    @sql ||= begin
      if subqueries.any?
        cte = <<~SQL.strip
          WITH #{subqueries.map { |q| "#{q.name} AS (#{q.to_sql})" }.join(', ')}
        SQL
      end

      ref_table = IndexedEnvelopeResourceReference.arel_table

      resource_column, subresource_column =
        if reverse_ref
          %i[subresource_uri resource_uri]
        else
          %i[resource_uri subresource_uri]
        end

      relation = subresource_uri_values ? ref_table : table
      relation = relation.where(condition) if condition

      if subresource_uri_values && subresource_uri_values != [ANY_VALUE]
        conditions = subresource_uri_values.map do |value|
          ref_table[subresource_column].matches("%#{value}%")
        end

        relation = relation.where(combine_conditions(conditions, :or))
      end

      if ref && subresource_uri_values.nil?
        relation = relation
          .join(ref_table)
          .on(table[:@id].eq(ref_table[subresource_column]))
      end

      if ref
        relation =
          relation
            .where(ref_table[:path].eq(ref))
            .project(ref_table[resource_column].as('resource_uri'))
      else
        relation = relation.skip(skip) if skip
        relation = relation.take(take) if take
        relation = relation.project(*projections)
      end

      if ref.nil? && with_metadata
        envelope_resources = EnvelopeResource.arel_table
        envelopes = Envelope.arel_table
        owners = Organization.arel_table.alias(:owners)
        publishers = Organization.arel_table.alias(:publishers)

        relation = relation
          .join(envelope_resources)
          .on(envelope_resources[:id].eq(table[:envelope_resource_id]))
          .join(envelopes)
          .on(envelopes[:id].eq(envelope_resources[:envelope_id]))
          .join(owners, Arel::Nodes::OuterJoin)
          .on(owners[:id].eq(envelopes[:organization_id]))
          .join(publishers, Arel::Nodes::OuterJoin)
          .on(publishers[:id].eq(envelopes[:publishing_organization_id]))
          .project(
            envelopes[:created_at],
            envelopes[:updated_at],
            owners[:_ctid].as('owned_by'),
            publishers[:_ctid].as('published_by'),
          )
      end

      [cte, relation.to_sql].join(' ').strip
    end
  end

  private

  def build(node)
    combine_conditions(build_node(node), find_operator(node))
  end

  def build_array_condition(key, value)
    value = SearchValue.new([value]) unless value.is_a?(SearchValue)
    return table[key].not_eq([]) if value.items == [ANY_VALUE]

    datatype = TYPES.fetch(context.dig(key, '@type'), :string)

    if value.items.size == 2 && datatype != :string
      range = Range.new(*value.items)
      return Arel::Nodes::ArrayAccess.new(table[key], 1).between(range)
    end

    operator = value.operator == :and ? :contains : :overlap
    table[key].send(operator, value.items)
  end

  def build_condition(key, value)
    reverse_ref = key.starts_with?('^')
    key = key.tr('^', '')
    column = columns_hash[key]
    context_entry = context[key]
    raise "Unsupported property: `#{key}`" unless context_entry || column

    context_entry ||= {}

    if context_entry['@type'] == '@id'
      return build_subquery_condition(key, value, reverse_ref)
    end

    return IMPOSSIBLE_CONDITION unless column

    search_value = build_search_value(value)
    match_type = search_value.match_type if search_value.is_a?(SearchValue)
    fts_condition = match_type.nil? || match_type == 'search:contain'

    if %w[@id ceterms:ctid].include?(key)
      build_id_condition(key, search_value.items)
    elsif context_entry['@container'] == '@language'
      if fts_condition
        build_fts_conditions(key, search_value)
      else
        build_like_condition(key, search_value.items, match_type)
      end
    elsif context_entry['@type'] == 'xsd:string'
      if fts_condition
        build_fts_condition('english', key, search_value.items)
      else
        build_like_condition(key, search_value.items, match_type)
      end
    elsif column.array
      build_array_condition(key, search_value)
    else
      build_scalar_condition(key, search_value)
    end
  end

  def build_from_array(node)
    node.map { |item| build(item) }
  end

  def build_from_hash(node)
    node = node.fetch('search:value', node)
    return build_from_array(node) if node.is_a?(Array)

    if (term_group = node['search:termGroup'])
      conditions = build_from_hash(node.except('search:termGroup'))
      return conditions << build(term_group)
    end

    node.map do |key, value|
      next if key == 'search:operator'

      build_condition(key, value)
    end.compact
  end

  def build_fts_condition(config, key, term)
    return table[key].not_eq(nil) if term == ANY_VALUE

    if term.is_a?(Array)
      conditions = term.map { |t| build_fts_condition(config, key, t) }
      return combine_conditions(conditions, :or)
    end

    term = term.fetch('search:value') if term.is_a?(Hash)
    quoted_config = Arel::Nodes.build_quoted(config)

    translated_column = Arel::Nodes::NamedFunction.new(
      'translate',
      [
        table[key],
        Arel::Nodes.build_quoted('/.'),
        Arel::Nodes.build_quoted(' ')
      ]
    )

    translated_term = Arel::Nodes::NamedFunction.new(
      'translate',
      [
        Arel::Nodes.build_quoted(term),
        Arel::Nodes.build_quoted('/.'),
        Arel::Nodes.build_quoted(' ')
      ]
    )

    column_vector = Arel::Nodes::NamedFunction.new(
      'to_tsvector',
      [quoted_config, translated_column]
    )

    query_vector = Arel::Nodes::NamedFunction.new(
      'plainto_tsquery',
      [quoted_config, translated_term]
    )

    Arel::Nodes::InfixOperation.new('@@', column_vector, query_vector)
  end

  def build_fts_conditions(key, value)
    conditions = value.items.map do |item|
      if item.is_a?(Hash)
        conditions = item.map do |locale, term|
          name = "#{key}_#{locale.tr('-', '_').downcase}"
          column = columns_hash[name]
          next IMPOSSIBLE_CONDITION unless column

          config =
            if locale.starts_with?('es')
              'spanish'
            elsif locale.starts_with?('fr')
              'french'
            else
              'english'
            end

          build_fts_condition(config, name, term)
        end
      elsif item.is_a?(SearchValue)
        build_fts_condition('english', key, item.items)
      elsif item.is_a?(String)
        build_fts_condition('english', key, item)
      else
        raise "FTS condition should be either an object or a string, `#{item}` is neither"
      end
    end.flatten

    combine_conditions(conditions, value.operator)
  end

  def build_id_condition(key, values)
    conditions = values.map do |value|
      if full_id_value?(key, value)
        table[key].eq(value)
      else
        table[key].matches("%#{value}%")
      end
    end

    combine_conditions(conditions, :or)
  end

  def build_like_condition(key, values, match_type)
    conditions = values.map do |value|
      value =
        case match_type
        when 'search:endsWith' then "%#{value}"
        when 'search:exactMatch' then "%#{value}%"
        when 'search:startsWith' then "#{value}%"
        else raise "Unsupported search:matchType: `#{matchType}`"
        end

      table[key].matches(value)
    end

    combine_conditions(conditions, :or)
  end

  def build_node(node)
    case node
    when Array then build_from_array(node)
    when Hash then build_from_hash(node)
    else raise "Either an array or object is expected, `#{node}` is neither"
    end
  end

  def build_scalar_condition(key, value)
    if %w[@id ceterms:ctid].include?(key)
      build_id_condition(key, value.items)
    else
      table[key].in(value.items)
    end
  end

  def build_search_value(value)
    case value
    when Array
      items =
        if value.first.is_a?(String)
          value
        else
          value.map { |item| build_search_value(item) }
        end

      SearchValue.new(items, :or)
    when Hash
      if (internal_value = value['search:value']).present?
        SearchValue.new(
          Array(internal_value),
          find_operator(value),
          value['search:matchType']
        )
      else
        SearchValue.new([value])
      end
    when String
      SearchValue.new([value])
    else
      value
    end
  end

  def build_subquery_condition(key, value, reverse)
    subquery_name = generate_subquery_name(key)

    subqueries << CtdlQuery.new(
      value,
      name: subquery_name,
      ref: key,
      reverse_ref: reverse
    )

    table[:'@id'].in(Arel.sql("(SELECT resource_uri FROM #{subquery_name})"))
  end

  def combine_conditions(conditions, operator)
    conditions.inject { |result, condition| result.send(operator, condition) }
  end

  def find_operator(node)
    return :or if node.is_a?(Array)

    node['search:operator'] == 'search:orTerms' ? :or : :and
  end

  def full_id_value?(key, value)
    case key
    when '@id' then valid_bnode?(value) || valid_uri?(value)
    when 'ceterms:ctid' then valid_ceterms_ctid?(value)
    else false
    end
  end

  def generate_subquery_name(key)
    value = [name, key.tr(':', '_')].compact.join('_')

    indices = subqueries.map do |subquery|
      match_data = /#{value}_?(?<index>\d+)?/.match(subquery.name)
      match_data['index'].to_i if match_data
    end

    last_index = indices.compact.sort.last
    return value unless last_index

    "#{value}_#{last_index + 1}"
  end

  def subresource_uri_values
    return unless ref

    @subresource_uri_values ||= begin
      search_value = build_search_value(query)
      items = search_value.items if search_value.is_a?(SearchValue)
      items if items&.first.is_a?(String)
    end
  end

  def valid_bnode?(value)
    !!UUID.validate(value[2..value.size - 1])
  end

  def valid_ceterms_ctid?(value)
    !!UUID.validate(value[3..value.size - 1])
  end

  def valid_uri?(value)
    URI.parse(value).is_a?(URI::HTTP)
  rescue URI::InvalidURIError
    false
  end
end
