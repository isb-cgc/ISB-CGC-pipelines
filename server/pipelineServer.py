import webapp2
from paste import httpserver


class ListJobs(webapp2.RequestHandler):
	def get(self):
		pass


class Job(webapp2.RequestHandler):
	def post(self):
		# submits a job
		pass

	def get(self):
		pass

	def put(self):
		pass

	def delete(self):
		pass


app = webapp2.WSGIApplication([
	(r'/jobs', ListJobs),
	(r'/jobs/(\d+)', Job),
], debug=True)


def main():
	httpserver.serve(app, host='0.0.0.0', port='8080')


if __name__ == "__main__":
	main()
