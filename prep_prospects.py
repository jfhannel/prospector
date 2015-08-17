import google, csv, sys, time
import threading
import time
import logging

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
    		if not row in self.people:
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
        


prospects = csv.reader(open(sys.argv[1],'rU'))

header = prospects.next()

fullname_col = -1
firstname_col = -1
lastname_col = -1
company_col = -1
title_col = -1
domain_col = -1
for i in xrange(len(header)):
	c = header[i].lower()

	if 'domain' in c:
		domain_col = i
		continue

	if 'name' in c:
		if fullname_col < 0 and 'full' in c:
			fullname_col = i
			continue
		if firstname_col < 0 and 'first' in c or 'given' in c:
			firstname_col = i
			continue
		if lastname_col < 0 and 'last' in c or 'family' in c or 'surname' in c:
			lastname_col = i
			continue
	
	if company_col < 0 and 'company' in c:
		company_col = i
		continue
	if title_col < 0 and 'title' in c:
		title_col = i
		continue

for i in xrange(len(header)):
	c = header[i].lower()
	if company_col < 0 and 'business' in c:
		company_col = i
		continue
	if title_col < 0 and 'role' in c:
		title_col = i
		continue

def get_prospect(s, pool, company, name, title, domain):
	logging.debug('Waiting to join the pool')
	with s:
		thread_name = threading.currentThread().getName()
		pool.makeActive(thread_name)
		if not len(domain):
			try:
				domainsearch = google.search(company)
				domain = domainsearch.next()
				time.sleep(0.1)
				while 'wiki' in domain.lower() and not 'wiki' in company:
					domain = domainsearch.next()
				w = domain.find('www')
				if w >= 0:
					domain = domain[w+4:]
				w = domain.find('http')
				if w >= 0:
					domain = domain[w+7:]
				if domain[-1] == '/':
					domain = domain[:-1]		

			except:
				e = sys.exc_info()[0]
				print "ERROR: ", str(e)
				
		outrow = [name, company, domain, title]
		print outrow
		pool.addPerson(outrow)	

		pool.makeInactive(thread_name)
	

out = csv.writer(open(sys.argv[2], 'w'))
outheader = ['Full Name', 'Company', 'Domain', 'Title']
out.writerow(outheader)
pool = ThreadPool(out)
s = threading.Semaphore(1)
for row in prospects:
	if company_col >= 0:
		company = row[company_col]
		title = row[title_col] if title_col >= 0 else ''
		domain = row[domain_col] if domain_col >= 0 else ''
		fullname = ''
		if fullname_col >= 0:
			fullname = row[fullname_col]
		elif firstname_col >= 0 and lastname_col >= 0:
			fullname = row[firstname_col]+' '+row[lastname_col]
		elif firstname_col >= 0:
			fullname = row[firstname_col]
		elif lastname_col >= 0:
			fullname = row[lastname_col]


		t = threading.Thread(target=get_prospect, name=fullname, args=(s,pool,company,fullname,title,domain))
		t.start()




