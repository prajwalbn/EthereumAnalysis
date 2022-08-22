from mrjob.job import MRJob
from mrjob.step import MRStep

class Top10_Job2(MRJob):
	def mapper_initial_aggregation(self, _, line):
		field = line.split(',')
		try:
			if len(field) == 7:
				to_address = field[2]
				value = int(field[3])
				yield to_address, (1,value)
			elif len(field) == 5:
				address = field[0]
				yield (address, (2,1)) 
		except:
			pass
	def reducer_aggregator(self, key, values):
		check = False
		join_value = []
		for i in values:
			if i[0]==1:
				join_value.append(i[1])
			elif i[0] == 2:
				check = True
		if check:
			yield (key, sum(join_value))

	def mapper_joining(self, key,value):
		yield (None, (key,value))

	def rreducer_sorting(self, _, keys):
		top_10 = sorted(keys, reverse = True, key = lambda x: x[1])
		for i in top_10[:10]:
			yield (i[0], i[1])

	def steps(self):
		return [MRStep(mapper = self.mapper_initial_aggregation, reducer=self.reducer_aggregator), MRStep(mapper = self.mapper_joining, reducer = self.reducer_sorting)]

if __name__ == '__main__':
	Top10_Job2.run()
			
