import argparse
from lib.pipelines.schema import PipelineSchema


def generate(args, config):
	parser = argparse.ArgumentParser()
	parser.add_argument("--cmd", required=True)
	parser.add_argument("--image", required=True)
	parser.add_argument("--logsPath", required=True)
	parser.add_argument("--preemptible", required=False, action="store_true", default=False)

	args = parser.parse_args(args=args)

	testSchema = PipelineSchema('test', args.tag, config)
	testSchema.setLogOutput(args.logsPath)

	testSchema.setCmd(args.cmd)
	testSchema.setImage(args.image)

	testSchema.setMem(1)
	testSchema.setCpu(1)

	if args.preemptible:
		testSchema.setPreemptible(True)

	return testSchema
