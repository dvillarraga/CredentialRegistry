require 'indexed_envelope_resource_reference'

# A flattened out version of an envelope resource's payload
class IndexedEnvelopeResource < ActiveRecord::Base
  belongs_to :envelope_resource
  has_many :references,
           class_name: 'IndexedEnvelopeResourceReference',
           foreign_key: :resource_id
end
