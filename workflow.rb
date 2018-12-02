require 'rbbt-util'
require 'rbbt/workflow'
require 'rbbt/network/paths'

Misc.add_libdir if __FILE__ == $0

#require 'rbbt/sources/NetworkEfficiency'

module NetworkEfficiency
  extend Workflow

  input :network, :tsv, "Network file in adjacency TSV format"
  input :start, :string, "start"
  input :eend, :string, "end"
  task :node2node => :array do |network,start, eend|
    max_steps = 100
    d = Paths.dijkstra(network, start, eend, max_steps)
    d
  end

  input :network, :tsv, "Network file in adjacency TSV format"
  input :blocked, :array, "Nodes that are removed from the network"
  task :distances => :tsv do |network, blocked|
    set_info :nodes, network.collect.flatten.uniq.length

    blocked = [] if blocked.nil?
    nodes = network.collect.flatten.uniq - blocked
    if blocked and blocked.any?
      old_network = network
      network = old_network.annotate({})
      old_network.each do |n,v|
        next if blocked.include? n
        network[n] = v
      end
    end

    nodes.each do |n|
      network[n] ||= []
    end

    distances = TSV.setup({}, :key_field => "Start node", :fields => nodes, :type => :list, :cast => :to_i)
    max_steps = 100
    nodes.each do |start_node|
      distances[start_node] ||= []
      nodes.each do |end_node|
        next if start_node == end_node
        d = Paths.dijkstra(network, start_node, end_node, max_steps)
        next if d.nil?
        distances[start_node][end_node] = d.length
      end
    end

    distances
  end

  dep :distances
  task :network_efficiency => :float do
    tsv = step(:distances).load
    n = step(:distances).info[:nodes]

    sum = 0

    tsv.each do |node, distances|
      distances.each do |distance|
        next if distance.nil?
        sum += 1.0/distance
      end
    end
    
    sum.to_f / (n * (n-1))
  end

  dep :network_efficiency
  dep :network_efficiency, :blocked => []
  task :pci => :float do
    ne = dependencies.first.load
    n = dependencies.last.load

    1.0 - (ne / n)
  end

  input :drug_targets, :tsv, "Drugs and targets in :flat TSV format"
  dep :pci, :blocked => :placeholder, :compute => :bootstrap do |jobname, options|
    drug_targets = options[:drug_targets]
    drugs = drug_targets.keys
    jobs = []
    drugs.each do |drug1|
      drugs.each do |drug2|
        next if drug1 == drug2
        next if drug1 > drug2
        blocked = drug_targets[drug1] + drug_targets[drug2]
        jobs << {:inputs => options.merge(:blocked => blocked.sort.uniq), :jobname => [drug1, drug2] * " + "}
      end
    end
    jobs
  end
  task :pci_drug_battery => :tsv do
    tsv = TSV.setup({}, :key_field => "Combination", :fields => ["PCI"], :type => :single, :cast => :to_f)
    dependencies.each do |dep|
      name = dep.clean_name
      tsv[name] = dep.load
    end
    tsv
  end

  input :drug_targets, :tsv, "Drugs and targets in :flat TSV format"
  dep :pci, :blocked => :placeholder, :compute => :bootstrap do |jobname, options|
    nodes = options[:network].collect.flatten.uniq
    jobs = []
    nodes.each do |node|
      blocked = [node]
      jobs << {:inputs => options.merge(:blocked => blocked), :jobname => node}
    end
    jobs
  end
  task :pci_importance => :tsv do
    tsv = TSV.setup({}, :key_field => "Combination", :fields => ["PCI"], :type => :single, :cast => :to_f)
    dependencies.each do |dep|
      name = dep.clean_name
      tsv[name] = dep.load
    end
    tsv
  end

end

#require 'NetworkEfficiency/tasks/basic.rb'

#require 'rbbt/knowledge_base/NetworkEfficiency'
#require 'rbbt/entity/NetworkEfficiency'

