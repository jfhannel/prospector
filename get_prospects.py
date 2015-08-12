import csv, sys
import threading
import logging
import requests

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-9s) %(message)s',)

class ThreadPool(object):
    def __init__(self, out):
        super(ThreadPool, self).__init__()
        self.active = []
        self.people = []
        self.peopleLock = threading.Lock()
        self.lock = threading.Lock()
        self.out = out
    def addPerson(self, row):
    	with self.peopleLock:
    		self.people.append(row)
    def makeActive(self, name):
        with self.lock:
            self.active.append(name)
            # logging.debug('Running: %s', self.active)
    def makeInactive(self, name):
        with self.lock:
            self.active.remove(name)
            # logging.debug('Running: %s', self.active)

    def __del__(self):
    	for row in self.people:
    		self.out.writerow(row)            


def get_prospect(s, pool, company, name, title, domain):
	logging.debug('Waiting to join the pool')
	with s:
		thread_name = threading.currentThread().getName()
		pool.makeActive(thread_name)
		try:
			url = 'https://0be1045ef4f60825c701ec141f687cec:@prospector.clearbit.com/v1/people/search'
			payload = {'domain': domain}
			if len(title):
				payload['title'] = title
			elif len(name):
				payload['name'] = name
			else:
				raise 'No name or title'


			people = requests.get(url, params=payload)
			result = people.json()
			print result
			if type(result) == type({}):
				if 'error' in result.keys():
					outrow = [name, company, domain, title, '', '', '', '', result['error']['message']]
					#import pdb;pdb.set_trace()
					pool.addPerson(outrow)
			elif type(result) == type([]) and len(result):
				#import pdb; pdb.set_trace()
				for r in result:
					outrow = [name, company, domain, title, r['email'], r['verified'], r['linkedin'], r['id'], '']
					pool.addPerson(outrow)
			else:
				outrow = [name, company, domain, title, '', '', '', '', '']
				pool.addPerson(outrow)					

		except:
			e = sys.exc_info()[0]
			print "ERROR: ", str(e)

		pool.makeInactive(thread_name)
	

prospects = csv.reader(open(sys.argv[1],'rU'))

header = prospects.next()

fullname_col = 0
company_col = 1
domain_col = 2
title_col = 3


out = csv.writer(open(sys.argv[2], 'w'))
outheader = ['Full Name', 'Company', 'Domain', 'Title', 'Email', 'Verified', 'Linkedin', 'Clearbit ID', 'Error']
out.writerow(outheader)
pool = ThreadPool(out)
s = threading.Semaphore(5)
for row in prospects:
	company = row[company_col]
	title = row[title_col]
	fullname = row[fullname_col]
	domain = row[domain_col]
	w = domain.find('www')
	if w >= 0:
		domain = domain[w+4:]
	w = domain.find('http')
	if w >= 0:
		domain = domain[w+7:]
	if domain[-1] == '/':
		domain = domain[:-1]
	
	t = threading.Thread(target=get_prospect, name=fullname, args=(s,pool,company,fullname,title,domain))
	t.start()





