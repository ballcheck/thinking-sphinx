require 'spec_helper'

describe ThinkingSphinx::RealTime::Populator do
  describe '.populate' do
    let(:populator) { double('populator') }
    it 'passes all arguments to #new' do
      expect(ThinkingSphinx::RealTime::Populator).to receive(:new).with("foo", "bar", "blegga").and_return(populator)
      expect(populator).to receive(:populate)

      ThinkingSphinx::RealTime::Populator.populate "foo", "bar", "blegga"
    end
  end

  context 'with limit' do
    let(:limit) { 9 }
    let(:scope) { double('scope') }
    let(:index) { double('index', scope: scope, name: nil, path: nil) }
    let(:populator) { ThinkingSphinx::RealTime::Populator.new index, limit }
    let(:transcriber) { double('transcriber') }
    describe '#populate' do
      it 'observes limit' do
        batch1 = [1, 2, 3, 4, 5]
        batch2 = [6, 7, 8, 9, 10]

        allow(scope).to receive(:find_in_batches).and_yield(batch1).and_yield(batch2)
        # move into before_each?
        allow(populator).to receive(:transcriber).and_return(transcriber)
        allow(populator).to receive(:remove_files)
        allow(populator).to receive(:instrument) # dry up?
        expect(transcriber).to receive(:copy).with(*batch1)
        expect(transcriber).to receive(:copy).with(*batch2[0..-2])

        populator.populate
      end
    end
  end
end
