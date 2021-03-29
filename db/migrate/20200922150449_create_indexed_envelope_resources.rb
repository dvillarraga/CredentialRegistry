class CreateIndexedEnvelopeResources < ActiveRecord::Migration[5.2]
  def change
    create_table :indexed_envelope_resources do |t|
      t.string :'@id', null: false
      t.string :'@type', null: false
      t.string :'ceterms:ctid'
      t.references :envelope_resource,
                   foreign_key: { on_delete: :cascade },
                   null: false
      t.datetime :created_at, null: false
      t.jsonb :payload, default: '{}', null: false

      t.index :'@id', name: 'i_ctdl_id', unique: true
      t.index :'@id',
              name: 'i_ctdl_id_trgm',
              opclass: { :'@id' => :gin_trgm_ops },
              using: :gin
      t.index :'@type', name: 'i_ctdl_type'
      t.index :'ceterms:ctid', name: 'i_ctdl_ceterms_ctid', unique: true
      t.index :'ceterms:ctid',
              name: 'i_ctdl_ceterms_ctid_trgm',
              opclass: { :'ceterms:ctid' => :gin_trgm_ops },
              using: :gin
    end
  end
end
