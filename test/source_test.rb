require File.dirname(__FILE__) + '/test_helper'

class Person < ActiveRecord::Base
end
class SourceTest < Test::Unit::TestCase
  context "a file source" do
    context "with delimited data" do
      setup do
        control = ETL::Control::Control.parse(
          File.dirname(__FILE__) + '/control/delimited.ctl')
        configuration = {
          :file => 'data/delimited.txt',
          :parser => :delimited
        }
        definition = self.definition + [:sex]
    
        source = ETL::Control::FileSource.new(
                  control, configuration, definition)
        @rows = source.collect { |row| row }
      end
      should "find 3 rows in the delimited file" do
        assert_equal 3, @rows.length
      end
    end
  end
  
  context "a file source with a glob" do
    setup do
      control = ETL::Control::Control.parse(
        File.dirname(__FILE__) + '/control/multiple_delimited.ctl')
      configuration = {
        :file => 'data/multiple_delimited_*.txt',
        :parser => :delimited
      }

      source = ETL::Control::FileSource.new(
                control, configuration, definition)
      @rows = source.collect { |row| row }
    end
    should "find 6 rows in total" do
      assert_equal 6, @rows.length
    end
  end
  
  context "a file source with an absolute path" do
    setup do
      FileUtils.cp(File.dirname(__FILE__) + '/control/data/delimited.txt', 
        '/tmp/delimited_abs.txt')

      control = ETL::Control::Control.parse(File.dirname(__FILE__) + 
        '/control/delimited_absolute.ctl')
      configuration = {
        :file => '/tmp/delimited_abs.txt',
        :parser => :delimited
      }

      definition = self.definition + [:sex]

      source = ETL::Control::FileSource.new(control, configuration, definition)
      @rows = source.collect { |row| row }
    end
    should "find 3 rows" do
      assert_equal 3, @rows.length
    end
  end

  context "a file source with the run relative path option set" do
    
    setup do

      unless File.directory?(File.dirname(__FILE__) + '/data_relative')
        FileUtils.mkdir(File.dirname(__FILE__) + '/data_relative')
      end

      FileUtils.cp(
        File.dirname(__FILE__) + '/control/data/delimited.txt',
        File.dirname(__FILE__) + '/data_relative/delimited_relative.txt'
        )

      control = ETL::Control::Control.parse(File.dirname(__FILE__) + 
        '/control/delimited_relative.ctl')

      configuration = {
        :file => 'data_relative/delimited_relative.txt',
        :parser => :delimited
      }

      definition = self.definition + [:sex]

      source = ETL::Control::FileSource.new(control, configuration, definition)
      @rows = source.collect { |row| row }
    end

    teardown do
      # FileUtils.rm_rf(File.dirname(__FILE__) + '/data_relative')
    end
    
    should "find 3 rows" do
      assert_equal 3, @rows.length
    end
  end
  
  context "multiple sources" do
    setup do
      control = ETL::Control::Control.parse(File.dirname(__FILE__) + 
        '/control/multiple_source_delimited.ctl')
      @rows = control.sources.collect { |source| source.collect { |row| row }}.flatten!
    end
    should "find 12 rows" do
      assert_equal 12, @rows.length
    end
  end
  
  context "a database source" do
    setup do
      control = ETL::Control::Control.parse(
                  File.dirname(__FILE__) + '/control/delimited.ctl')
      configuration = {
        :database => 'etl_unittest',
        :target => :operational_database,
        :table => 'people',
      }
      definition = [ 
        :first_name,
        :last_name,
        :ssn,
      ]
      @source = ETL::Control::DatabaseSource.new(
                  control, configuration, definition)
    end
    should "set the local file for extraction storage" do
      assert_match(%r{source_data/localhost/etl_unittest/people/\d+.csv}, 
        @source.local_file.to_s)
    end
    should_eventually "find 1 row" do
      Person.delete_all
      assert 0, Person.count
      Person.create!(:first_name => 'Bob', 
                     :last_name => 'Smith', 
                     :ssn => '123456789')
      assert 1, Person.count
      rows = @source.collect { |row| row }
      assert 1, rows.length
    end
  end
  
  context "a file source with an xml parser" do
    setup do
      control = ETL::Control::Control.parse(File.dirname(__FILE__) + 
        '/control/xml.ctl')
      @rows = control.sources.collect{ 
        |source| source.collect { |row| row }}.flatten!
    end
    should "find 2 rows" do
      assert_equal 2, @rows.length
    end
  end

  context "a model source" do
    setup do
      control = ETL::Control::Control.parse(
        File.dirname(__FILE__) + '/control/model_source.ctl')
      configuration = {

      }
      definition = [
        :first_name,
        :last_name,
        :ssn
      ]
    end
    should_eventually "find n rows" do
      
    end
  end
  
  def definition
    [ 
      :first_name,
      :last_name,
      :ssn,
      {
        :name => :age,
        :type => :integer
      }
    ]
  end
end
