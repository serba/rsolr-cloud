require 'spec_helper.rb'

RSpec.describe RSolr::Cloud::Connection do
  before do
    @zk_in_solr = ZK.new
    delete_with_children(@zk_in_solr, '/live_nodes')
    wait_until(10) do
      !@zk_in_solr.exists?('/live_nodes')
    end
    @zk_in_solr.create('/live_nodes')

    ['192.168.1.21:8983_solr',
     '192.168.1.22:8983_solr',
     '192.168.1.23:8983_solr',
     '192.168.1.24:8983_solr'
    ].each do |node|
      @zk_in_solr.create("/live_nodes/#{node}", '', mode: :ephemeral)
    end
    @zk = ZK.new
    @subject = RSolr::Cloud::Connection.new @zk
  end

  let(:client) { double.as_null_object }

  let(:http) { double(Net::HTTP).as_null_object }

  it 'should configure Net::HTTP with one of live node in select or update request.' do
    expect(@subject.instance_variable_get(:@live_nodes).sort).to eq(
      ['http://192.168.1.21:8983/solr',
       'http://192.168.1.22:8983/solr',
       'http://192.168.1.23:8983/solr',
       'http://192.168.1.24:8983/solr'].sort)
    expect(Net::HTTP).to receive(:new) do |host, port|
      expect(host).to be_one_of(['192.168.1.21', '192.168.1.22', '192.168.1.23', '192.168.1.24'])
      expect(port).to eq(8983)
      http
    end

    expect(http).to receive(:request) do |request|
      expect(request.path).to eq('/solr/collection1/select?q=*:*')
      double.as_null_object
    end
    @subject.execute client, collection: 'collection1', method: :get, path: 'select', query: 'q=*:*'

    expect(http).to receive(:request) do |request|
      expect(request.path).to eq('/solr/collection1/update')
      expect(request.body).to eq('the data')
      double.as_null_object
    end
    @subject.execute client, collection: 'collection1',
                             method: :post,
                             path: 'update',
                             data: 'the data'
  end

  it 'should remove downed node and add recovered node.' do
    @zk_in_solr.delete('/live_nodes/192.168.1.21:8983_solr')
    expect { @subject.instance_variable_get(:@live_nodes).sort }.to become_soon(
      ['http://192.168.1.22:8983/solr',
       'http://192.168.1.23:8983/solr',
       'http://192.168.1.24:8983/solr'].sort)
    @zk_in_solr.create('/live_nodes/192.168.1.21:8983_solr', mode: :ephemeral)
    expect { @subject.instance_variable_get(:@live_nodes).sort }.to become_soon(
      ['http://192.168.1.21:8983/solr',
       'http://192.168.1.22:8983/solr',
       'http://192.168.1.23:8983/solr',
       'http://192.168.1.24:8983/solr'].sort)
  end

  it 'should obey url scheme.' do
    @zk_in_solr.create('/clusterprops.json', '{"urlScheme": "https"}')
    @subject = RSolr::Cloud::Connection.new @zk
    expect(@subject.instance_variable_get(:@url_scheme)).to eq('https')
    expect(@subject.instance_variable_get(:@live_nodes).sort).to eq(
      ['https://192.168.1.21:8983/solr',
       'https://192.168.1.22:8983/solr',
       'https://192.168.1.23:8983/solr',
       'https://192.168.1.24:8983/solr'].sort)
  end

  after do
    @zk_in_solr.close if @zk_in_solr
    @zk.close         if @zk
  end
end
