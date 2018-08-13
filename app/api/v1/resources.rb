require 'mountable_api'
require 'envelope'
require 'delete_token'
require 'batch_delete_envelopes'
require 'envelope_builder'
require 'entities/envelope'
require 'helpers/shared_helpers'
require 'helpers/community_helpers'
require 'helpers/envelope_helpers'
require 'v1/publish'

module API
  module V1
    # Implements all the endpoints related to resources
    class Resources < MountableAPI
      mounted do # rubocop:disable Metrics/BlockLength
        helpers SharedHelpers
        helpers CommunityHelpers
        helpers EnvelopeHelpers

        include API::V1::Defaults
        include API::V1::Publish

        before do
          params[:envelope_community] = select_community
        end

        resource :resources do
          desc 'Publishes a new envelope',
               http_codes: [
                 { code: 201, message: 'Envelope created' },
                 { code: 200, message: 'Envelope updated' }
               ]
          params do
            use :update_if_exists
            use :skip_validation
          end
          post do
            envelope, errors = EnvelopeBuilder.new(
              params,
              update_if_exists: update_if_exists?,
              skip_validation: skip_validation?
            ).build

            if errors
              json_error! errors, [:envelope, envelope.try(:resource_schema_name)]
            else
              present envelope, with: API::Entities::Envelope
              if envelope.created_at != envelope.updated_at
                status(:ok)
              else
                status(:created)
              end
            end
          end

          desc 'Return a resource.'
          params do
            requires :id, type: String, desc: 'Resource id.'
          end
          after_validation do
            find_envelope
          end
          get ':id', requirements: { id: /(.*)/i } do
            present @envelope.inner_resource_from_graph(params[:id])
          end

          desc 'Updates an existing envelope'
          params do
            requires :id, type: String, desc: 'Resource id.'
            use :skip_validation
          end
          after_validation do
            find_envelope
          end
          put ':id', requirements: { id: /(.*)/i } do
            sanitized_params = params.dup
            sanitized_params.delete(:id)
            envelope, errors = EnvelopeBuilder.new(
              sanitized_params,
              envelope:        @envelope,
              skip_validation: skip_validation?
            ).build

            if errors
              json_error! errors, [:envelope, envelope.try(:community_name)]
            else
              present envelope, with: API::Entities::Envelope
            end
          end

          desc 'Marks an existing envelope as deleted'
          params do
            requires :id, type: String, desc: 'Resource id.'
          end
          after_validation do
            find_envelope
            params[:envelope_id] = @envelope.envelope_id
          end
          delete ':id', requirements: { id: /(.*)/i } do
            validator = JSONSchemaValidator.new(params, :delete_envelope)
            if validator.invalid?
              json_error! validator.error_messages, :delete_envelope
            end

            BatchDeleteEnvelopes.new([@envelope], DeleteToken.new(params)).run!

            body false
            status :no_content
          end
        end
      end
    end
  end
end
