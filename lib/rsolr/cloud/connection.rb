module RSolr
  module Cloud
    # RSolr connection adapter for SolrCloud
    class Connection < RSolr::Connection
      include MonitorMixin

      ZNODE_LIVE_NODES = '/live_nodes'.freeze
      ZNODE_CLUSTER_PROPS = '/clusterprops.json'.freeze

      def initialize(zk)
        super()
        @zk = zk
        init_url_scheme
        init_live_node_watcher
      end

      def execute(client, request_context)
        collection_name = request_context[:collection]
        raise 'The :collection option must be specified.' unless collection_name
        path  = request_context[:path].to_s
        query = request_context[:query]
        query = query ? "?#{query}" : ''
        url   = select_node(collection_name)
        raise RSolr::Cloud::Error::NotEnoughNodes unless url
        request_context[:uri] = RSolr::Uri.create(url).merge(path + query)
        super(client, request_context)
      end

      private

      def init_url_scheme
        @url_scheme = 'http'
        if @zk.exists?(ZNODE_CLUSTER_PROPS)
          json, _stat = @zk.get(ZNODE_CLUSTER_PROPS)
          props = JSON.parse(json)
          @url_scheme = props['urlScheme'] || 'http'
        end
      end

      def select_node(collection)
        synchronize { @live_nodes.sample + '/' + collection }
      end

      def init_live_node_watcher
        @zk.register(ZNODE_LIVE_NODES) do
          update_live_nodes
        end
        update_live_nodes
      end

      def update_live_nodes
        synchronize do
          @live_nodes = []
          @zk.children(ZNODE_LIVE_NODES, watch: true).each do |node|
            # "/" between host_and_port part of url and context is replaced with "_" in ZK
            @live_nodes << @url_scheme + '://' + node.tr('_', '/')
          end
        end
      end
    end
  end
end
